from flask import Flask, render_template
import mysql.connector
from mysql.connector import Error

app = Flask(__name__, template_folder='html')

def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host='db',
            database='metro',
            user='root',
            password='example'
        )
        return connection
    except Error as e:
        print(f"Connexion error with the database: {e}")
        return None

@app.route('/')
def index():
    connection = get_database_connection()
    if connection is None:
        return "Connexion error with the database", 500

    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT nomcourtligne, nomarret, destination, departfirsttrain, departsecondtrain FROM passages") # SQL request to the database
    data = cursor.fetchall()
    cursor.close()
    connection.close()

    # Sort data by line_name and station_name
    data = sorted(data, key=lambda x: (x['nomcourtligne'], x['nomarret']))

    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

