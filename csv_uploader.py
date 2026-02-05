from flask import Flask, request, render_template_string, flash, redirect, url_for
import pandas as pd
import os
from werkzeug.utils import secure_filename
from src.db import get_conn

app = Flask(__name__)
app.secret_key = 'csv_uploader_key'

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CSV to Strategy Settings</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input[type="file"] { padding: 8px; }
        button { background: #4CAF50; color: white; padding: 10px 20px; border: none; cursor: pointer; }
        button:hover { background: #45a049; }
        .message { padding: 10px; margin: 10px 0; border-radius: 4px; }
        .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .preview { margin-top: 20px; }
        table { border-collapse: collapse; width: 100%; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>📊 CSV to Strategy Settings</h1>

    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            {% for category, message in messages %}
                <div class="message {{ category }}">{{ message }}</div>
            {% endfor %}
        {% endif %}
    {% endwith %}

    <form method="POST" action="/upload" enctype="multipart/form-data">
        <div class="form-group">
            <label>Upload Strategy CSV File:</label>
            <input type="file" name="file" accept=".csv" required>
            <br><small>CSV should contain columns: strategy_name, big_candle_tf, small_candle_tf, from_date, to_date</small>
        </div>
        <button type="submit">📤 Upload & Insert</button>
    </form>

    {% if csv_data %}
    <div class="preview">
        <h2>📊 CSV Preview ({{ csv_data|length }} strategies)</h2>
        <table>
            <tr>
                {% for col in csv_columns %}
                <th>{{ col }}</th>
                {% endfor %}
            </tr>
            {% for row in csv_data[:5] %}
            <tr>
                {% for col in csv_columns %}
                <td>{{ row[col] if row[col] is not none else '' }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
        </table>
        {% if csv_data|length > 5 %}
        <p><em>Showing first 5 rows...</em></p>
        {% endif %}

        <form method="POST" action="/confirm_insert">
            <input type="hidden" name="confirm" value="yes">
            <button type="submit">✅ Confirm Insert into Database</button>
            <a href="/"><button type="button">⬅️ Upload Different File</button></a>
        </form>
    </div>
    {% endif %}
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part', 'error')
        return redirect(request.url)

    file = request.files['file']
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(request.url)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            # Read and validate CSV
            df = pd.read_csv(filepath)

            # Check required columns
            required_cols = ['strategy_name', 'big_candle_tf', 'small_candle_tf', 'from_date', 'to_date']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                flash(f'Missing required columns: {", ".join(missing_cols)}', 'error')
                return redirect(request.url)

            # Store data in session for confirmation
            csv_data = df.to_dict('records')
            csv_columns = list(df.columns)

            return render_template_string(HTML_TEMPLATE,
                                       csv_data=csv_data,
                                       csv_columns=csv_columns,
                                       message=f'✅ CSV uploaded successfully! Found {len(csv_data)} strategies.',
                                       status="info")

        except Exception as e:
            flash(f'Error reading CSV: {str(e)}', 'error')
            return redirect(request.url)

    flash('Invalid file type. Please upload a CSV file.', 'error')
    return redirect(request.url)

@app.route('/confirm_insert', methods=['POST'])
def confirm_insert():
    if request.form.get('confirm') != 'yes':
        flash('Invalid request', 'error')
        return redirect(url_for('index'))

    # For this simple app, we'll assume the CSV was just uploaded
    # In a real app, you'd store it in session or database
    # For now, let's look for the most recent CSV in uploads
    try:
        upload_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.endswith('.csv')]
        if not upload_files:
            flash('No CSV file found. Please upload first.', 'error')
            return redirect(url_for('index'))

        # Use the most recent file
        latest_file = max(upload_files, key=lambda x: os.path.getctime(os.path.join(UPLOAD_FOLDER, x)))
        filepath = os.path.join(UPLOAD_FOLDER, latest_file)

        df = pd.read_csv(filepath)

        # Insert into strategy_settings
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Clear existing data (optional)
                cur.execute("DELETE FROM strategy_settings")

                # Insert all strategies from CSV
                for i, (_, row) in enumerate(df.iterrows()):
                    # Fill missing columns with defaults
                    strategy_data = {
                        'strategy_name': row.get('strategy_name', f'strategy_{i+1}'),
                        'big_candle_tf': row.get('big_candle_tf', 15),
                        'small_candle_tf': row.get('small_candle_tf', 5),
                        'preferred_breakout_type': row.get('preferred_breakout_type', 'full_candle_breakout'),
                        'breakout_threshold_pct': row.get('breakout_threshold_pct', 60),
                        'option_entry_price_cap': row.get('option_entry_price_cap', 80),
                        'hedge_entry_price_cap': row.get('hedge_entry_price_cap', 50),
                        'num_entry_legs': row.get('num_entry_legs', 4),
                        'num_hedge_legs': row.get('num_hedge_legs', 1),
                        'sl_percentage': row.get('sl_percentage', 20),
                        'eod_time': row.get('eod_time', '15:20:00'),
                        'no_of_lots': row.get('no_of_lots', 1),
                        'lot_size': row.get('lot_size', 75),
                        'hedge_exit_entry_ratio': row.get('hedge_exit_entry_ratio', 50),
                        'hedge_exit_multiplier': row.get('hedge_exit_multiplier', 3),
                        'leg_profit_pct': row.get('leg_profit_pct', 84),
                        'portfolio_profit_target_pct': row.get('portfolio_profit_target_pct', 2),
                        'portfolio_stop_loss_pct': row.get('portfolio_stop_loss_pct', 2),
                        'portfolio_capital': row.get('portfolio_capital', 900000),
                        'max_reentry_rounds': row.get('max_reentry_rounds', 3),
                        'sl_type': row.get('sl_type', 'regular_system_sl'),
                        'box_sl_trigger_pct': row.get('box_sl_trigger_pct', 25),
                        'box_sl_hard_pct': row.get('box_sl_hard_pct', 35),
                        'reentry_breakout_type': row.get('reentry_breakout_type', 'full_candle_breakout'),
                        'one_m_candle_tf': row.get('one_m_candle_tf', 1),
                        'entry_candle': row.get('entry_candle', 1),
                        'switch_pct': row.get('switch_pct', 20.00),
                        'width_sl_pct': row.get('width_sl_pct', 40.00),
                        'from_date': row.get('from_date', '2025-01-01'),
                        'to_date': row.get('to_date', '2025-01-31')
                    }

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
                        ON CONFLICT (strategy_name) DO UPDATE SET
                            big_candle_tf = EXCLUDED.big_candle_tf,
                            small_candle_tf = EXCLUDED.small_candle_tf,
                            preferred_breakout_type = EXCLUDED.preferred_breakout_type,
                            breakout_threshold_pct = EXCLUDED.breakout_threshold_pct,
                            option_entry_price_cap = EXCLUDED.option_entry_price_cap,
                            hedge_entry_price_cap = EXCLUDED.hedge_entry_price_cap,
                            num_entry_legs = EXCLUDED.num_entry_legs,
                            num_hedge_legs = EXCLUDED.num_hedge_legs,
                            sl_percentage = EXCLUDED.sl_percentage,
                            eod_time = EXCLUDED.eod_time,
                            no_of_lots = EXCLUDED.no_of_lots,
                            lot_size = EXCLUDED.lot_size,
                            hedge_exit_entry_ratio = EXCLUDED.hedge_exit_entry_ratio,
                            hedge_exit_multiplier = EXCLUDED.hedge_exit_multiplier,
                            leg_profit_pct = EXCLUDED.leg_profit_pct,
                            portfolio_profit_target_pct = EXCLUDED.portfolio_profit_target_pct,
                            portfolio_stop_loss_pct = EXCLUDED.portfolio_stop_loss_pct,
                            portfolio_capital = EXCLUDED.portfolio_capital,
                            max_reentry_rounds = EXCLUDED.max_reentry_rounds,
                            sl_type = EXCLUDED.sl_type,
                            box_sl_trigger_pct = EXCLUDED.box_sl_trigger_pct,
                            box_sl_hard_pct = EXCLUDED.box_sl_hard_pct,
                            reentry_breakout_type = EXCLUDED.reentry_breakout_type,
                            one_m_candle_tf = EXCLUDED.one_m_candle_tf,
                            entry_candle = EXCLUDED.entry_candle,
                            switch_pct = EXCLUDED.switch_pct,
                            width_sl_pct = EXCLUDED.width_sl_pct,
                            from_date = EXCLUDED.from_date,
                            to_date = EXCLUDED.to_date
                    """, tuple(strategy_data.values()))

                conn.commit()

        flash(f'✅ Successfully inserted {len(df)} strategies into strategy_settings table!', 'success')
        return redirect(url_for('index'))

    except Exception as e:
        flash(f'❌ Error inserting data: {str(e)}', 'error')
        return redirect(url_for('index'))

if __name__ == '__main__':
    print("📊 CSV to Strategy Settings uploader starting...")
    print("📡 Visit: http://127.0.0.1:5002")
    app.run(host='0.0.0.0', port=5002, debug=True)