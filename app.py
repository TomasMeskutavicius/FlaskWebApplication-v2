from fastapi import FastAPI, Path, Query, HTTPException, Response, status
from typing import Optional
import sqlite3
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import uvicorn

app = FastAPI()

#app = Flask(__name__)

JOBS = [{
    'id': 1,
    'title': 'Data Analyst',
    'location': 'Bengaluru, India',
    'salary': '$10,00,000'
}, {
    'id': 2,
    'title': 'Data Scientist',
    'location': 'Delhi, India',
    'salary': '$15,00,000'
}, {
    'id': 3,
    'title': 'Frontend Engineer',
    'location': 'Remote',
}, {
    'id': 4,
    'title': 'Backend Engineer',
    'location': 'San Francisco, USA',
    'salary': '$150,000'
}]


@app.get("/")
def home():
    return {"Data": "Testing"}


@app.get("/get-all-employees")
def get_all_employees():
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute("select * from telephones")
    data = cur.fetchall()
    cur.close()
    con.close()

    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="No persons found, DB is empty")
    else:
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in data]
        return Response(content=json.dumps(data),
                        media_type="application/json")


@app.get("/get-all-names")
def get_all_names():
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute("select Name from telephones")
    data = cur.fetchall()
    cur.close()
    con.close()

    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="No person names found in DB")
    else:
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in data]
        return Response(content=json.dumps(data),
                        media_type="application/json")


@app.get("/get-all-last-names")
def get_all_last_names():
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute("select LastName from telephones")
    data = cur.fetchall()
    cur.close()
    con.close()

    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="No person last names found in DB")
    else:
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in data]
        return Response(content=json.dumps(data),
                        media_type="application/json")


@app.get("/get-details")
def get_details(*, name: Optional[str] = None, LastName: Optional[str] = None):
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    if LastName is None and name is not None:
        cur.execute("select * from telephones where Name=?", (name, ))
    elif LastName is not None and name is not None:
        cur.execute("select * from telephones where Name=? AND LastName=?",
                    (name, LastName))
    elif LastName is not None and name is None:
        cur.execute("select * from telephones where LastName=?", (LastName, ))

    data = cur.fetchall()
    cur.close()
    con.close()

    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Person not found")
    else:
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in data]
        return Response(content=json.dumps(data),
                        media_type="application/json")


@app.post("/create-person")
def create_person(Team: str,
                  Name: str,
                  LastName: str,
                  S_N: str,
                  CurrIMEI: str,
                  Orderdate: str,
                  WarrPerriod: int,
                  OldIMEI: Optional[str] = None,
                  WarrEndDate: Optional[str] = None,
                  IMEI2: Optional[str] = None):
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute(
        "CREATE table IF NOT EXISTS  telephones(Team, Name, LastName, S_N, CurrIMEI, Orderdate, WarrEndDate, OldIMEI, IMEI2, WarrPerriod)"
    )
    cur.execute("select Name from telephones where Name=? AND LastName=?",
                (Name, LastName))
    data = cur.fetchall()
    if not data:
        WarrEndDate = (datetime.strptime(Orderdate, "%Y-%m-%d") +
                       relativedelta(years=WarrPerriod)).strftime("%Y-%m-%d")
        data = [(Team, Name, LastName, S_N, CurrIMEI, Orderdate, WarrEndDate,
                 OldIMEI, IMEI2, WarrPerriod)]
        cur.executemany(
            "INSERT INTO telephones VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            data)
        con.commit()
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Name already exists.")
    con.close()
    return {"Success": "Data created!"}


@app.put("/update-person")
def update_person(Name: str,
                  LastName: str,
                  S_N: str,
                  CurrIMEI: str,
                  Orderdate: str,
                  Team: Optional[str] = None,
                  WarrPerriod: Optional[int] = None,
                  OldIMEI: Optional[str] = None,
                  IMEI2: Optional[str] = None):
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute(
        "select Name, LastName from telephones where Name=? AND LastName=?",
        (Name, LastName))
    data = cur.fetchall()
    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provided Name and Last Name does not exist!.")
    else:
        if S_N is not None:
            person_to_update = [(S_N, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET S_N=? WHERE Name=? AND LastName=?",
                person_to_update)
        if CurrIMEI is not None:
            person_to_update = [(CurrIMEI, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET CurrIMEI=? WHERE Name=? AND LastName=?",
                person_to_update)
        if Orderdate is not None:
            WarrEndDate = datetime.strptime(
                Orderdate, "%Y-%m-%d") + relativedelta(years=WarrPerriod)
            person_to_update = [(Orderdate, WarrEndDate, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET Orderdate=?, WarrEndDate=? WHERE Name=? AND LastName=?",
                person_to_update)
        if Team is not None:
            person_to_update = [(Team, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET Team=? WHERE Name=? AND LastName=?",
                person_to_update)
        if WarrPerriod is not None:
            person_to_update = [(WarrPerriod, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET WarrPerriod=? WHERE Name=? AND LastName=?",
                person_to_update)
        if OldIMEI is not None:
            person_to_update = [(OldIMEI, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET OldIMEI=? WHERE Name=? AND LastName=?",
                person_to_update)
        #if WarrEndDate is not None:
        #    person_to_update = [(WarrEndDate, Name, LastName)]
        #    cur.executemany("UPDATE telephones SET WarrEndDate=? WHERE Name=? AND LastName=?", person_to_update)
        if IMEI2 is not None:
            person_to_update = [(IMEI2, Name, LastName)]
            cur.executemany(
                "UPDATE telephones SET IMEI2=? WHERE Name=? AND LastName=?",
                person_to_update)

    con.commit()
    con.close()
    return {"Success": "Data updated!"}


@app.delete("/delete-person")
def delete_person(Name: str, LastName: str):
    con = sqlite3.connect("telephones.db")
    cur = con.cursor()
    cur.execute(
        "select Name, LastName from telephones where Name=? AND LastName=?",
        (Name, LastName))
    data = cur.fetchall()
    if not data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provided Name and Last Name does not exist!.")
    else:
        person_to_delete = [(Name, LastName)]
        cur.executemany("DELETE FROM telephones WHERE Name=? AND LastName=?",
                        person_to_delete)
        con.commit()

    con.close()
    return {"Success": "Data deleted!"}


@app.get("/about")
def about():
    return {"Tomo PIRMASIS API!!!!!"}


@app.get("/get-all-christmas-names")
def get_all_christmas_names():

    def check_database(db_file):
        # Check if the database file exists
        if not os.path.isfile(db_file):
            print(f"Database '{db_file}' does not exist.")
            return False
        return True

    def check_table_exists(db_file, table_name):
        # Connect to the database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Query to check if the table exists
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        )
        table_exists = cursor.fetchone() is not None

        # Close the connection
        conn.close()

        return table_exists

    # Example usage
    db_file = 'christmas.db'
    table_name = 'christmas'

    if check_database(db_file):
        if check_table_exists(db_file, table_name):
            print(f"Table '{table_name}' exists in '{db_file}'.")
        else:
            print(f"Table '{table_name}' does not exist in '{db_file}'.")

    con = sqlite3.connect("christmas.db")
    cur = con.cursor()

    cur.execute("CREATE table IF NOT EXISTS christmas(Name, CurrentYear)")

    #data = [
    #    ("Tomas", ""),
    #    ("Justina", ""),
    #    ("Jelena", ""),
    #    ("Gintaras", ""),
    #    ("Neila", ""),


#
#    ("Vasilijus", ""),
#    ("Tatjana", ""),
#
#    ("Gintas", ""),
#    ("Kristina", ""),
#    ("Dovydas", ""),
#    ("Goda", ""),
#    ]
#cur.executemany("INSERT INTO christmas VALUES(?, ?)", data)
#con.commit()

#cur.execute("select Name from christmas")
#data = cur.fetchall()
#cur.close()
#con.close()

#if not data:
#    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No person names found in DB")
#else:
#    columns = [col[0] for col in cur.description]
#    data = [dict(zip(columns, row)) for row in data]
#   return Response(content=json.dumps(data), media_type="application/json")


@app.get("/get-christmas-detailsss")
def get_christmas_details(*, name: Optional[str] = None):
    con = sqlite3.connect("christmas.db")
    cur = con.cursor()
    #if name is not None:

    #random()

    #person_to_update = [("Team", name)]
    #cur.executemany("UPDATE christmas SET CurrentYear=? WHERE Name=? ", person_to_update)

    #cur.execute("select Name from christmas where CurrentYear =''")

    #cur.execute("select * from christmas where Name=?", (name,))
    #elif name is None:
    #cur.execute("select * from christmas")

    data = cur.fetchall()
    con.commit()
    cur.close()
    con.close()

    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Person not found")
    else:
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in data]
        return Response(content=json.dumps(data),
                        media_type="application/json")


@app.delete("/delete-db")
def delete_db(Name: str):
    os.remove(Name)


uvicorn.run(app, port=8080, host="0.0.0.0")
