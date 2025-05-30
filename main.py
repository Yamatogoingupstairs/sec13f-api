from flask import Flask, request, jsonify
import process_module  # 軽量な処理関数だけ入れたモジュール

app = Flask(__name__)

@app.route('/run-sec13f', methods=['POST'])
def run_script():
    try:
        result = process_module.run_for_year(2023)  # 固定 or 軽い処理
        return jsonify({"status": "success", "result": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

import os

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # デフォルト10000
    app.run(host='0.0.0.0', port=port)

