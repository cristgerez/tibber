### Tibber Assigment by Cristobal Gerez

- Install requirements:
```cli
pip install -r requirements.txt
```

- Add database config to a jason in the root of the folder:
```json 
{
  "dbname": "dbname",
  "user": "user",
  "password": "password",
  "host": "host",
  "port": "port"
}
```

To run a job, type any of this in your terminal:

```cli
python main.py --job currencies
python main.py --job batch
python main.py --job view
python main.py --job all  # Default
```

