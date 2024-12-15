

For this bootcamp i am using the cloud shell console, a free google cloud virtual machine. It has installed many tools like docker and kubernetes and laguajes like python and java and can help to beginners because they dont need to try with software installation and so improve their performance or if you need a fast enviroment.

One thing to consider for the third week if you want to use the cloud shell is the cross-host Jupyter Notebook. When you connect to the spark-iceberg container, add the next configuration to avoid the problem.

- Run
    vim /root/.jupyter/jupyter_notebook_config.py
- Paste and save changes:
    # Allow cross-origin requests
    c.NotebookApp.allow_origin = '*'

    # Allow credentials
    c.NotebookApp.allow_credentials = True
- Restart:
    Don't forget restart the spark-iceberg container
