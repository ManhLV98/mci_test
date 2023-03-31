## Project name: MCI TEST
## Set up 
```
$ Set up wsl2 in windown 10
```
![wsl](images/WSL.png)
```
$ Install Pycharm
```
![pycharm](images/Pycharm.png)
```
$ Install Python3
    1. sudo apt update
    2. python3 -V 
```
![python3](images/Python3.png)
```
$ Install Pip3
    1.sudo apt install python3-pip
    2.pip3 --version
```
![pip3](images/pip3.png)
```
$ Install Env python
    1.sudo apt update
    2.pip3 install virtualenv
    3.virtualenv mci_env
    4.pip3 install -r requirements.txt
    5.source /home/user/mci_env/bin/activate
```
![active_source](images/active_source.png)
## Design 
```
$ Upload data from ggsheet ->S3
```
![active_source](images/s3.png)
```
$ Design 3 procedures : 
    1. public.proc_create_table():
        - Drop table if table is exist
        - Create tables
    2. public.proc_s3_to_tb(name_tb,path):
        - Load data from S3 to Redshift(STG)
    3. public.proc_music_sample():
        - clean and standardize table
        - Load data to Redshift(DWH)
```
![active_source](images/procedure.png)
```
$ Create 6 file in Project(mci_test):
    - dwh.cfg: Info redshift and S3 role 
    - requirements: library env python3
    - sql_queries: SQL in redshift 
        - Contain 3 procedures:
            + public.proc_create_table()
            + public.proc_s3_to_tb(name_tb,path)
            + public.proc_music_sample()
    - test.ipynb: Test data from S3
    - create_table: Create table in redshift
        - call public.proc_create_table()
    - etl:
        + Load data from s3->Redshift(STG)
          - call public.proc_s3_to_tb(name_tb,path)
        + Redshift(STG)-> Redshift(DWH)
          - call public.proc_music_sample()
```
## Run project
```
$ jupyter notebook(test data from S3):
```
![create_tb](images/test_data.png)
```
$ run create_tb.py(init table in redshift):
```
![create_tb](images/create_tb.png)
```
$ run etl.py(load data):
```
![etl_dwh](images/etl_dwh.png)
## Diagram:
![diagram](images/diagram.png)
## Results
```
$ Table log_data:
```
![log_data](images/log_data.png)
```
$ Table song_data:
```
![song_data](images/song_data.png)
```
$ Table artists:
```
![artists](images/artists.png)
```
$ Table users:
```
![users](images/users.png)
```
$ Table time:
```
![time](images/time.png)
```
$ Table songs:
```
![songs](images/songs.png)
```
$ Table songplays:
```
![songplays](images/songplays.png)
## License
Author: ManhLV

