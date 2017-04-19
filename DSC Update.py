from bs4 import BeautifulSoup
import pypyodbc
import traceback
import getpass
import re
import openpyxl as opxl
import os

#pyinstaller command
#pyinstaller --noconfirm --onefile --name "Refresh DSC List" --distpath "\\core\data\CPA\Data\AccessManagement\DSC Update script" "H:\Projects\SharePoint DSC Update Scripts\DSC Update.py"
# USES PYTHON 3.5.3 32-BIT ONLY!!

def main() :

	HTMLsource = r'\\?\UNC\core\data\CPA\Data\AccessManagement\DSCLIST.EMAIL.HTML'
	errorFlag = False
	cnxn, cursor = None, None
	
	newDSCList = getNewDSCs(HTMLsource)
	
	try :
		print('Connecting to SharePoint DSC database...')
		pypyodbc.lowercase = False
		cnxn = pypyodbc.connect('DSN=DSCs')
		cursor = cnxn.cursor()
		for row in cursor.tables() :
			print(row.table_name)

		currentDSCList = getCurrentDSCs( cursor )
		currentDSCList = removeDSCs(currentDSCList, newDSCList, cursor)
		appendDSCs(currentDSCList, newDSCList, cursor)
		
		print('DSC list update completed, closing database connection.')
	except Exception as err :
		errorFlag = True
		print('{} - Database connection error'.format(err))
		traceback.print_exc()
	finally :
		if cursor :
			cursor.close()
		if cnxn :
			cnxn.close()
		if not errorFlag :
			input('Update completed successfully, press ENTER to close window.')
		elif errorFlag :
			input('Update script has encountered an unexpected error, press ENTER to abort and close window.')

class DSC :
	
	def __init__(self, name, scope ,division) :
		self.name = name.upper()
		self.scope = scope.upper()
		self.division = division.upper()
		
	def __str__(self) :
		return '{} - {} - {}'.format(self.division, self.scope, self.name)
		
	def __eq__(self, other) :
		if isinstance(other, self.__class__) :
			if self.name == other.name and self.scope == other.scope and self.division == other.division :
				return True
		return False
		
	def __ne__(self, other) :
		return not self.__eq__(other)
		
def getNewDSCs( source ) :

	print('Pulling new DSC list data...')

	DSClist = []
	division = None
	scope = None
	
	scopeFlag = True
	nameFlag = not scopeFlag
	
	with open(source, 'r') as html :
		for line in html :
			if 'class="l byline"' in line :
				division = re.search('>Division=(.*)<', line).group(1)
			if 'class="l data"' in line and scopeFlag:
				scope = re.search('>(.*)<', line).group(1)
				scopeFlag = not scopeFlag
				nameFlag = not scopeFlag
			elif 'class="l data"' in line and nameFlag:
				name = re.search('>(.*)<', line).group(1)
				DSClist.append(DSC(name, scope, division))
				scopeFlag = not scopeFlag
				nameFlag = not scopeFlag
				#print(line, end='')
	
	print('New DSC data pulled successfully.')
	return DSClist

def getCurrentDSCs( cursor ) :
	
	print('Pulling DSC data from current list...')
	currentDSCList = []

	try :
		cursor.execute('SELECT * FROM [DSC LIST];')
		for row in cursor.fetchall() :
			if row[0] not in currentDSCList :
				tempDSC = DSC(division=row[0], scope=row[1], name=row[2])
				#print(tempDSC)
				currentDSCList.append(tempDSC)
	except Exception as err :
		print('Error reading current DSC list from database'.format(err))
		raise
		
	print('Current DSC data pulled successfully.')
	
	return currentDSCList
	
def removeDSCs( currentList, newList, cursor ) :
	'''Return a list of all DSCs that need to be removed from the database'''
	print('Checking for DSCs to remove...')

	tempList = currentList
	
	currentList = [x for x in tempList  if tempList.count(x) == 1]
	deleteList = [x for x in tempList if tempList.count(x) > 1 ]
	
	for person in currentList :
		if person not in newList :
			deleteList.append(person)
	
	try :
		for person in deleteList :
			print('Removing DSC {}'.format(person))
			result = cursor.execute("DELETE FROM [DSC List] WHERE [DIVISION]=? AND [SCOPE]=? AND [NAME (LAST, FIRST)]=?;", (person.division, person.scope, person.name))
		return currentList
		#print('DSC delete query completed')
	except :
		raise
			
def appendDSCs( currentList, newList, cursor ) :
	'''Return a list of all DSCs that need to be added to the database'''
	print('Checking for DSCs to add...')
	appendList = []
	for person in newList :
		if person not in currentList :
			appendList.append(person)
	
	try :
		for person in appendList :
			print('Adding DSC {}'.format(person))
			result = cursor.execute("INSERT INTO [DSC List] ([DIVISION], [SCOPE], [NAME (LAST, FIRST)]) VALUES (?, ?, ?);", (person.division, person.scope, person.name))
		#print('DSC append query completed')
	except :
		raise
	
main()