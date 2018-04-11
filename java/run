#mpjrun.sh -np 4 target/classes/hpc_instagram_geoprocessing/App
javac -cp .:$MPJ_HOME/lib/mpj.jar *.java
echo "np=2"
mpjrun.sh -np 2 HPCInstagramGeoprocessing ../data/melbGrid.json ../data/mediumInstagram.json
echo "np=4"
mpjrun.sh -np 4 HPCInstagramGeoprocessing ../data/melbGrid.json ../data/mediumInstagram.json
