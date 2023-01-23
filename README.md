# **pow** (Part of World)
## initialize a Postregsql's db with hierarchized territories (as geometry)
> ###this is a fork of :thumbsup: LAPOSTE/BCAA project, which aims to facilitate business targeting with choropleth maps

## to install the application,
first declare 3 variables, as :
```
*POW_DIR_ROOT* which is the root of the cloned code, from https://github.com/christopheprudent/pow.git
*POW_DIR_DATA* to implements trees for data
*POW_USER* to declare which user will be used to execute POW (adding code into its .bashrc)
```

after that, it's time to install packages, build the database, ...
log in with $POW_USER,
run:
* $POW_DIR_ROOT/bin/admin/install.sh                                -- packages
* $POW_DIR_ROOT/bin/admin/db.objects.d/db.objects.sh                -- build DB
