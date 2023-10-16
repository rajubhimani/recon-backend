#nohup uvicorn main:app --reload --host '0.0.0.0' --port 5000 > log.txt 2>log.txt &
nohup python -m gunicorn main:app -b 0.0.0.0:5000 -t 300 -w 4 -k uvicorn.workers.UvicornWorker > log.txt 2>log.txt &
