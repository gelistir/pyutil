::This is helpful when using the Gitbash on Windows
rm -rf env
conda create --yes -p env --file condalist.txt
env\Scripts\pip.exe install -r requirements.txt
