N=6

rm interleavings.txt
if [ $1 == "PRT" ]
then

	echo "Running All"
	for (( i = 1; i <= N; i++ )); do

		echo "===================PRT"$i"===============================" 

		javac java/$1$i.java
		cd java 

		echo "=================Testing================================="

		if [ $2 == 0 ]
		then
			echo "Monkey DB...."
			java -cp :postgresql-42.2.24.jre7.jar $1$i $2 $3
		elif [ $2 == 1 ]
		then
			echo "MySQL DB...."
			java -cp :mysql-connector-java-5.1.47.jar $1$i $2 $3
		elif [ $2 == 2 ]
		then
			echo "PgSQL DB...."
			java -cp :postgresql-42.2.24.jre7.jar $1$i $2 $3
		elif [ $2 == 3 ]
		then
			echo "MsSQL DB...."
			java -cp :mssql-jdbc-9.4.0.jre11.jar $1$i $2 $3
		else
			echo "Invalid database code!!"
		fi

		rm *.class
		cd ..

		echo "==================Assertions================================"

		python3 assert.py $1$i $2
	done
else

	javac java/$1.java
	cd java 

	echo "=================Testing================================="

	if [ $2 == 0 ]
	then
		echo "Monkey DB...."
		java -cp :mysql-connector-java-5.1.47.jar $1 $2 $3
	elif [ $2 == 1 ]
	then
		echo "MySQL DB...."
		java -cp :mysql-connector-java-5.1.47.jar $1 $2 $3
	elif [ $2 == 2 ]
	then
		echo "PgSQL DB...."
		java -cp :postgresql-42.2.24.jre7.jar $1 $2 $3
	elif [ $2 == 3 ]
	then
		echo "MsSQL DB...."
		java -cp :mssql-jdbc-9.4.0.jre11.jar $1 $2 $3
	else
		echo "Invalid database code!!"
	fi

	rm *.class
	cd ..

	echo "==================Assertions================================"

	python3 assert.py $1 $2

fi

