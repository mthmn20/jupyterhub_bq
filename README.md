Docker container for setting up JupyterHub server for pydata + google cloud workflows
=====================================================================================

Docker container to run a multi-user IPython/Jupyter notebook server on ubuntu 14.04. Includes JupyterHub, standard PyData libraries (matplotlib, pandas, sklearn, scipy, seaborn) and google app engine/cloud storage/biquery compatibility. Hosted on an ec2 micro instance and available at kadatasci.ml.

To use PAM authentication for the Notebook users, use the `add_user.sh` script from the scripts directory.

##Instructions

To modify the repo:

1. To modify configuration: make desired changes to requirements.txt or Dockerfile (note: changes to apt-get or pip installs will trigger a full rebuild, otherwise it will use the cache. A full rebuild is very, very slow.). To modify content: add your own notebooks to jupyterhub_bq/notebooks (see notes below**)
2. In the root of the repo, rebuild the docker image:
   - docker build -t amyskerry/jupyterhub .
3. Run a docker container with the newly built image (binding port 80 to 8000)
   - docker run -p 80:8000 -t amyskerry/jupyterhub
4. Login to KA jupyterhub environment
5. Open and execute test.ipynb --> Ensure that all modules import and that ipywidgets render appropriately
6. Commit changes to git repo
7. Changes will not go live until you ssh into the ec2 instance and build and run image there.

**Adding content:
You can temporarily create notebooks in the jupyterhub environment for development and testing purposes, but these notebooks will not be saved if the jupyterhub environment needs to be restarted. When you are ready to add persistent notebooks to the jupyterhub environment, check out the repo locally and add a subdirectory in jupyterhub_bq/notebooks (I suggest putting .ipynb files at top level and all supporting code, etc. in additional subdirs). Will need to rebuild image with the new files.

