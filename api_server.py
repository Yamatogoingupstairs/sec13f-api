from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run-sec13f', methods=['POST'])
def run_script():
    try:
        result = subprocess.run(
            ['python3', 'sec_13f_main.py', '--start_year', '2023', '--end_year', '2024'],
            capture_output=True, text=True
        )
        return jsonify({
            "status": "success" if result.returncode == 0 else "error",
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5001)
