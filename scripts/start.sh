prefect server start --host=0.0.0.0 &
prefect agent start -q 'default'
jupyter notebook --allow-root --ip=0.0.0.0 --port=8888 --no-browser