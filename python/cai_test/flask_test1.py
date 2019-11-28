# -*- coding: UTF-8 -*-

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return 'hello python22'

if __name__ == '__main__':
    app.run()