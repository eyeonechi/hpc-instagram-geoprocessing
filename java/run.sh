javac -cp .:$MPJ_HOME/lib/mpj.jar *.java
#echo "np=2"
#mpjrun.sh -np 2 HPCInstagramGeoprocessing ../data/melbGrid.json ../data/mediumInstagram.json
#echo "np=4"
#mpjrun.sh -np 4 HPCInstagramGeoprocessing ../data/melbGrid.json ../data/mediumInstagram.json
mpjrun.sh -np 2 CloudNine ../data/melbGrid.json ../data/tinyInstagram.json

#mpjrun.sh -np 1 -cp ./gson-2.2.2.jar:./commons-cli-1.3.1.jar:./commons-lang3-3.5.jar myMainClass -f smallTwitter.json
