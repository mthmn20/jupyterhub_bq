Docker container for setting up JupyterHub server for pydata + google cloud workflows
=====================================================================================

Docker container to run a multi-user IPython/Jupyter notebook server on ubuntu 14.04. Includes JupyterHub, standard PyData libraries (matplotlib, pandas, sklearn, scipy, seaborn) and google app engine/cloud storage/biquery compatibility. Hosted on an ec2 micro instance and available at kadatasci.ml.

To use PAM authentication for the Notebook users, use the `add_user.sh` script from the scripts directory.

##Instructions

To modify the repo:

1. Modifying repo:
   - To modify configuration: 
	- check out repo locally to build a test docker image on your own machine
	- make desired changes to requirements.txt or Dockerfile (note: changes to apt-get or pip installs will trigger a full rebuild, otherwise it will use the cache. A full rebuild is very, very slow.). 
   - To modify content: 
        - option 1: modify content by checking out repo locally and adding contents to /notebooks (will require same steps of sshing into host and rebuilding/running image)
        - option 2: login to kadatasci.ml and add your own notebooks to jupyterhub_bq/notebooks (files created and edited from the jupyterhub environment will be saved to disk on the host instance). If you go with this option, you can ignore the remaining steps (all changes will be auto-committed, but not pushed)
2. In the root of the repo, rebuild the docker image:
   - docker build -t amyskerry/jupyterhub .
3. Run a docker container with the newly built image (binding port 80 to 8000, and mounting host directory so that files edited in opt/shared_nbs persist on the host independent of the container)
   - docker run -p 80:8000 -t -v /home/ec2-user/jupyterhub_bq/notebooks:/opt/shared_nbs jupyterhub amyskerry/jupyterhub
4. Login to KA jupyterhub environment
5. Open and execute test.ipynb --> Ensure that all modules import and that ipywidgets render appropriately
6. Commit changes to git repo
7. Changes will not go live until you ssh into the ec2 instance and build and run image there.


