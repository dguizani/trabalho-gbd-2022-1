# PYTHON

**Version:** `3.9.12`

### `venv` Use In `git bash` Windows

**Installation**
`C:/python/3.9.12_x64/python.exe -m venv venv`

**Activate VENV**
`source ./venv/Scripts/activate`

**Update `pip` Python**
`python -m pip install --upgrade pip`

**Install Dependencies**
`pip install -r ./requirements.txt`

---

# CONTAINER MYSQL

### `.env` File

```.env
DB_PASS=1234
DB_NAME=dw_mysql
DB_PORT=9999
```

---

### `docker-compose` Command

**Up container**
`docker-compose -f ./docker-compose.yaml up -d`

**Down container**
`docker-compose -f ./docker-compose.yaml down`

---

# DW

### Execute

`python main.py`

---
