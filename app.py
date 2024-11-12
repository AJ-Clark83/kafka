from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Test Kafka consumer app running on Fly.io."

@app.route('/status')
def status():
    return "Kafka consumer is running."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
