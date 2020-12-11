from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def hello_world():
    return '<b>HELLO</b>'

with open(htmlfile.) as file:

@app.route('/fl')
def fl():
    render_template('index.html')

if__name__ == '__main__'
    app.run(debug=True)