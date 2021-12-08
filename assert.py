
import sys

name = sys.argv[1]
db_code = sys.argv[2]
db = ""
if db_code == "0":
	db = "monkey"
elif db_code == "1":
	db = "mysql"
elif db_code == "2":
	db = "pg"
elif db_code == "3":
	db = "sqlsv"


file = open("./text/"+db+"_"+name+"_iterations.txt")
writeFile = open("./interleavings.txt",'a')
yes = False

if name == 'PRT1':

	print("Searching in "+name+" for")
	search_word = "0(Studentcnt) \n1(UpdateStudent) \n2(Studentcnt)"
	print(search_word)

	if(search_word in file.read()):
		yes = True

elif name == 'PRT2':

	print("Searching in "+name+" for")
	search_word = "0(Studentcnt) \n4(Studentcnt)"
	print(search_word)

	if(search_word in file.read()):
		yes = True

elif name == 'PRT3':

	print("Searching in "+name+" for")
	search_word = "0(Studentcnt) \n1(UpdateStudent) \n2(Studentcnt)"
	print(search_word)

	if(search_word in file.read()):
		yes = True

elif name == 'PRT4':

	print("Searching in "+name+" for")
	search_word = "1(Studentcnt) \n1(DeleteSpecificStudent) \n2(Studentcnt)"
	print(search_word)

	if(search_word in file.read()):
		yes = True
	else:
		print("Searching in "+name+" for")
		search_word = "{Manoj }(SelectName) \n1(UpdateStudent) \n{Manoj** }(SelectName)"
		print(search_word)

		if(search_word in file.read()):
			yes = True

elif name == 'PRT5':

	print("Searching in "+name+" for")
	search_word = "1(Studentcnt) \n1(DeleteSpecificStudent) \n2(Studentcnt)"
	print(search_word)

	if(search_word in file.read()):
		yes = True

elif name == 'PRT6':

	print("Searching in "+name+" for")
	search_word = "Error!! \n"
	print(search_word)

	if(search_word in file.read()):
		yes = True


writeFile.write("\n================"+name+"================")
if yes == True:
	print("Expected Interleaving detected!!")
	writeFile.write("\nExpected Interleaving found!!")
else:
	print("Bad Luck!! Didn't observe expected interleaving!!")
	writeFile.write("\nBad Luck!! Didn't observe expected interleaving!!")

writeFile.close()




	
