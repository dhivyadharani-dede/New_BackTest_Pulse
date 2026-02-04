from flask import Flask, request, send_file, render_template, flash, redirect, url_for, session, jsonify
import pandas as pd
import os
import io
from werkzeug.utils import secure_filename
import sys
from pathlib import Path
import time

repo_root = Path(__file__).resolve().parents[0]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this in production

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def update_progress(step, message, progress_percent):
    """Update progress in session"""
    session['progress'] = {
        'step': step,
        'message': message,
        'progress': progress_percent,
        'timestamp': time.time()
    }
    session.modified = True

def get_progress():
    """Get current progress from session"""
    return session.get('progress', {
        'step': 'idle',
        'message': 'Ready to start',
        'progress': 0,
        'timestamp': time.time()
    })

@app.route('/progress')
def progress():
    """API endpoint to get current progress"""
    return jsonify(get_progress())

@app.route('/')
def index():
    # Check if there's uploaded data to display
    uploaded_data = None
    column_names = None
    show_confirm = False
    
    if 'uploaded_file' in session:
        try:
            df = pd.read_csv(session['uploaded_file'])
            uploaded_data = df.to_dict('records')
            column_names = list(df.columns)
            show_confirm = True
        except:
            # If file can't be read, clear session
            session.pop('uploaded_file', None)
    
    return render_template('index.html', 
                         uploaded_data=uploaded_data,
                         column_names=column_names,
                         show_confirm=show_confirm)

@app.route('/download_template')
def download_template():
    # Create a sample CSV template
    df = pd.DataFrame({
        'strategy_name': ['example_strategy'],
        'big_candle_tf': [15],
        'small_candle_tf': [5],
        'preferred_breakout_type': ['full_candle_breakout'],
        'breakout_threshold_pct': [60],
        'option_entry_price_cap': [80],
        'hedge_entry_price_cap': [50],
        'num_entry_legs': [4],
        'num_hedge_legs': [1],
        'sl_percentage': [20],
        'eod_time': ['15:20:00'],
        'no_of_lots': [1],
        'lot_size': [75],
        'hedge_exit_entry_ratio': [50],
        'hedge_exit_multiplier': [3],
        'leg_profit_pct': [84],
        'portfolio_profit_target_pct': [2],
        'portfolio_stop_loss_pct': [2],
        'portfolio_capital': [900000],
        'max_reentry_rounds': [3],
        'sl_type': ['regular_system_sl'],
        'box_sl_trigger_pct': [25],
        'box_sl_hard_pct': [35],
        'reentry_breakout_type': ['full_candle_breakout'],
        'one_m_candle_tf': [1],
        'entry_candle': [1],
        'switch_pct': [20.00],
        'width_sl_pct': [40.00],
        'from_date': ['2025-01-01'],
        'to_date': ['2025-02-02']
    })
    
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    return send_file(io.BytesIO(output.getvalue().encode()), download_name='strategy_template.csv', as_attachment=True, mimetype='text/csv')

def process_uploaded_csv(df):
    try:
        update_progress('database_prep', 'Preparing database...', 10)
        
        # Insert into strategy_settings
        with get_conn() as conn:
            with conn.cursor() as cur:
                update_progress('clearing_data', 'Clearing existing strategy data...', 20)
                # Clear existing strategies
                cur.execute("DELETE FROM strategy_settings")
                
                update_progress('inserting_strategies', f'Inserting {len(df)} strategies into database...', 30)
                
                for i, (_, row) in enumerate(df.iterrows()):
                    cur.execute("""
                        INSERT INTO strategy_settings (
                            strategy_name, big_candle_tf, small_candle_tf, preferred_breakout_type,
                            breakout_threshold_pct, option_entry_price_cap, hedge_entry_price_cap,
                            num_entry_legs, num_hedge_legs, sl_percentage, eod_time, no_of_lots,
                            lot_size, hedge_exit_entry_ratio, hedge_exit_multiplier, leg_profit_pct,
                            portfolio_profit_target_pct, portfolio_stop_loss_pct, portfolio_capital,
                            max_reentry_rounds, sl_type, box_sl_trigger_pct, box_sl_hard_pct,
                            reentry_breakout_type, one_m_candle_tf, entry_candle, switch_pct,
                            width_sl_pct, from_date, to_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['strategy_name'], row['big_candle_tf'], row['small_candle_tf'], 
                        row['preferred_breakout_type'], row['breakout_threshold_pct'], 
                        row['option_entry_price_cap'], row['hedge_entry_price_cap'], 
                        row['num_entry_legs'], row['num_hedge_legs'], row['sl_percentage'], 
                        row['eod_time'], row['no_of_lots'], row['lot_size'], 
                        row['hedge_exit_entry_ratio'], row['hedge_exit_multiplier'], 
                        row['leg_profit_pct'], row['portfolio_profit_target_pct'], 
                        row['portfolio_stop_loss_pct'], row['portfolio_capital'], 
                        row['max_reentry_rounds'], row['sl_type'], row['box_sl_trigger_pct'], 
                        row['box_sl_hard_pct'], row['reentry_breakout_type'], 
                        row['one_m_candle_tf'], row['entry_candle'], row['switch_pct'], 
                        row['width_sl_pct'], row['from_date'], row['to_date']
                    ))
                    
                    # Update progress for each strategy
                    progress = 30 + int((i + 1) / len(df) * 30)
                    update_progress('inserting_strategies', f'Inserted strategy {i+1}/{len(df)}: {row["strategy_name"]}', progress)
                
                conn.commit()
        
        update_progress('running_backtest', 'Running backtesting algorithms...', 70)
        
        # Clear previous results before running new backtest
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM strategy_run_results")
                conn.commit()
        
        # Run the strategy (assumes views are already refreshed)
        update_progress('running_strategy', 'Executing trading strategy...', 75)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("CALL sp_run_strategy()")
                conn.commit()
        
        update_progress('processing_results', 'Processing backtest results...', 90)
        
        update_progress('complete', 'Backtesting completed successfully!', 100)
        
        return {'status': 'success'}
        
    except Exception as e:
        update_progress('error', f'Error: {str(e)}', 0)
        return {'status': 'error', 'message': str(e)}

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            # Clear any previous results before processing new upload
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM strategy_run_results")
                        conn.commit()
            except:
                pass  # Ignore errors if table doesn't exist or connection fails
            
            # Read the CSV file
            df = pd.read_csv(filepath)
            
            # Store the dataframe in session for later processing
            # Since Flask sessions can't store DataFrames directly, we'll store the filepath
            session['uploaded_file'] = filepath
            
            # Convert to dict for template display
            uploaded_data = df.to_dict('records')
            column_names = list(df.columns)
            
            return render_template('index.html', 
                                 uploaded_data=uploaded_data,
                                 column_names=column_names,
                                 show_confirm=True)
            
        except Exception as e:
            flash(f'Error reading file: {str(e)}')
            return redirect(request.url)
    
    flash('Invalid file type')
    return redirect(request.url)

@app.route('/confirm_run')
def confirm_run():
    if 'uploaded_file' not in session:
        flash('No uploaded file found. Please upload a CSV first.')
        return redirect(url_for('index'))
    
    # Redirect to processing page
    return redirect(url_for('processing'))

@app.route('/processing')
def processing():
    return render_template('processing.html')

@app.route('/start_processing', methods=['POST'])
def start_processing():
    if 'uploaded_file' not in session:
        return jsonify({'status': 'error', 'message': 'No uploaded file found'})
    
    try:
        # Read the uploaded CSV
        df = pd.read_csv(session['uploaded_file'])
        
        # Process the CSV (this will update progress)
        result = process_uploaded_csv(df)
        
        return jsonify({'status': 'started'})
    except Exception as e:
        update_progress('error', f'Error starting processing: {str(e)}', 0)
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/cancel_upload')
def cancel_upload():
    # Clear uploaded file from session
    session.pop('uploaded_file', None)
    session.pop('progress', None)  # Also clear any progress data
    
    # Clear any previous results
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM strategy_run_results")
                conn.commit()
    except:
        pass  # Ignore errors if table doesn't exist or connection fails
    
    flash('Upload cancelled. You can now upload a new file.')
    return redirect(url_for('index'))

@app.route('/results')
def results():
    # Get results and analysis
    with get_conn() as conn:
        # Get all results
        results_df = pd.read_sql("SELECT * FROM strategy_run_results ORDER BY strategy_name, trade_date", conn)
        
        # Analysis: Daily metrics per strategy
        analysis_query = """
        SELECT
            r.strategy_name,
            r.trade_date,
            COUNT(*) as total_trades,
            SUM(r.pnl_amount) as total_pnl,
            MIN(r.pnl_amount) as worst_trade,
            MAX(r.pnl_amount) as best_trade
        FROM strategy_run_results r
        GROUP BY r.strategy_name, r.trade_date
        ORDER BY r.strategy_name, r.trade_date
        """
        analysis_df = pd.read_sql(analysis_query, conn)
        
        # Calculate overall strategy performance for top strategies
        overall_query = """
        SELECT
            strategy_name,
            SUM(total_trades) as total_trades,
            SUM(total_pnl) as total_pnl,
            AVG(total_pnl) as avg_daily_pnl,
            COUNT(DISTINCT trade_date) as trading_days
        FROM (
            SELECT
                r.strategy_name,
                r.trade_date,
                COUNT(*) as total_trades,
                SUM(r.pnl_amount) as total_pnl
            FROM strategy_run_results r
            GROUP BY r.strategy_name, r.trade_date
        ) daily
        GROUP BY strategy_name
        ORDER BY total_pnl DESC
        """
        overall_df = pd.read_sql(overall_query, conn)
        
        # Top 3 strategies
        top_strategies = overall_df.head(3).to_dict('records')
        for row in top_strategies:
            row['reason'] = f"High total PnL (₹{row['total_pnl']:.2f}) over {int(row['trading_days'])} trading days"
    
    return render_template('results.html', results=results_df, analysis=analysis_df.to_dict('records'), top_strategies=top_strategies)

@app.route('/download_analysis')
def download_analysis():
    with get_conn() as conn:
        # Daily analysis query
        daily_query = """
        SELECT
            r.strategy_name,
            r.trade_date,
            COUNT(*) as total_trades,
            SUM(r.pnl_amount) as total_pnl,
            MIN(r.pnl_amount) as worst_trade,
            MAX(r.pnl_amount) as best_trade
        FROM strategy_run_results r
        GROUP BY r.strategy_name, r.trade_date
        ORDER BY r.strategy_name, r.trade_date
        """
        daily_df = pd.read_sql(daily_query, conn)
        
        # Overall strategy summary
        overall_query = """
        SELECT
            strategy_name,
            SUM(total_trades) as total_trades,
            SUM(total_pnl) as total_pnl,
            AVG(total_pnl) as avg_daily_pnl,
            COUNT(DISTINCT trade_date) as trading_days
        FROM (
            SELECT
                r.strategy_name,
                r.trade_date,
                COUNT(*) as total_trades,
                SUM(r.pnl_amount) as total_pnl
            FROM strategy_run_results r
            GROUP BY r.strategy_name, r.trade_date
        ) daily
        GROUP BY strategy_name
        ORDER BY total_pnl DESC
        """
        overall_df = pd.read_sql(overall_query, conn)
        
        results_df = pd.read_sql("SELECT * FROM strategy_run_results ORDER BY strategy_name, trade_date", conn)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        results_df.to_excel(writer, index=False, sheet_name='Full Results')
        daily_df.to_excel(writer, index=False, sheet_name='Daily Analysis')
        overall_df.to_excel(writer, index=False, sheet_name='Strategy Summary')
        
        # Rankings sheet based on total PnL
        top_5 = overall_df.head(5).copy()
        top_5['Rank'] = range(1, len(top_5) + 1)
        top_5['Type'] = 'Top'
        bottom_5 = overall_df.tail(5).copy()
        bottom_5['Rank'] = range(len(overall_df) - 4, len(overall_df) + 1)
        bottom_5['Type'] = 'Bottom'
        rankings = pd.concat([top_5, bottom_5])
        rankings[['Rank', 'strategy_name', 'total_pnl', 'Type']].to_excel(writer, index=False, sheet_name='Rankings')
    
    output.seek(0)
    return send_file(output, download_name='strategy_analysis.xlsx', as_attachment=True)

@app.route('/download_results')
def download_results():
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM strategy_run_results ORDER BY strategy_name, trade_date", conn)
    
    # Compute no trade dates for each strategy
    no_trade_dates = []
    for strategy_name in df['strategy_name'].unique():
        strategy_df = df[df['strategy_name'] == strategy_name].copy()
        if not strategy_df.empty:
            # Ensure trade_date is date object for comparison
            strategy_df['trade_date'] = pd.to_datetime(strategy_df['trade_date']).dt.date
            min_date = min(strategy_df['trade_date'])
            max_date = max(strategy_df['trade_date'])
            # Generate date range as date objects
            all_dates = [d.date() for d in pd.date_range(start=min_date, end=max_date, freq='D')]
            existing_dates = set(strategy_df['trade_date'])
            missing_dates = [d for d in all_dates if d not in existing_dates]
            for md in missing_dates:
                no_trade_dates.append({'strategy_name': strategy_name, 'no_trade_date': md})
    
    no_trade_df = pd.DataFrame(no_trade_dates)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for strategy_name, group_df in df.groupby('strategy_name'):
            # Sort the data for each strategy sheet
            group_df = group_df.sort_values(by=['trade_date', 'expiry_date', 'entry_time', 'option_type', 'leg_type', 'strike'])
            # Sanitize sheet name: replace invalid characters and limit length
            safe_sheet_name = strategy_name.replace('/', '_').replace('\\', '_').replace('[', '_').replace(']', '_').replace('*', '_').replace('?', '_').replace(':', '_')[:31]
            group_df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
        # Add No trade date sheet
        no_trade_df.to_excel(writer, index=False, sheet_name='No trade date')
    output.seek(0)
    
    return send_file(output, download_name='strategy_results.xlsx', as_attachment=True)

@app.route('/health')
def health_check():
    """Health check endpoint for Docker and monitoring"""
    try:
        # Test database connection
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': time.time()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': time.time()
        }), 503

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)