import sqlite3
import datetime
import mysql.connector

sqlite_file = '/Users/BConn/Library/Messages/chat.db'
table_name = 'message' #iMessage history

id_column = 'ROWID'
text_column = 'text'
handle_column = 'handle_id'
distro_column = 'cache_roomnames'
distro_id = 'chat644931494225636'

# connecting to the database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

#Get the total number of messages in the group chat
c.execute('SELECT COUNT(*) {coi2} FROM {tn} WHERE {coi2}="chat644931494225636"'.\
          format(coi2=distro_column, tn=table_name, cn=handle_column))
total_distro_messages = c.fetchall()

#Remove the extra second item that is put in the tuple
denominator = []
for x in total_distro_messages:
    for y in x:
        denominator.append(y)

total_distro_messages = str(denominator[0])

print('The Distro has sent a total of ' + total_distro_messages + ' messages since December 2014.')
print("")
print('Individal contributions are as follows:')
print("")

# Obtaining each members messages of the distro
# 0 - Bryan, 2 - Crandall, 3 - Jim, 4 - Cole, 5 - Kenny, 6 - Jack, 7 - Kydes, 8 - Zach, 9 - Evan, 10 - Scotty, 11 - Greg
members = ['Bryan', 'Brant', 'Jimmy Mole', 'Cole', 'Kenny', 'Jack', 'Kydes', 'Fennius', 'Evan', 'Scotty', 'Greg']
results = []
for x in range(0,12):
    c.execute('SELECT COUNT(*) {coi2},{coi1} FROM {tn} WHERE {coi2}="chat644931494225636" AND {coi1}={a}'.\
          format(coi2=distro_column, coi1=handle_column, tn=table_name, cn=handle_column, a=x))
    total_messages = c.fetchall()
    results.append(total_messages)

#Remove the empty list that is encountered because of the '1' variable
results.pop(1)

#Replace the handle_id's with the actual names of people
list2 = []
list1 = results

#Converts List of Lists of Tuples to List of Tuples
for x in list1:
    for y in x:
        list2.append(y)

#Convert List of Tuples to List of Lists
list3 = [list(elem) for elem in list2]

#Switch items in each list so it's handle_id, messages
for x in list3:
    x.reverse()

#Replace handle_ids with names of actual distro members
u = 0
for x in list3:
    x[0] = members[u]
    u = u+1

#Separating names and messages sent into two separate lists
name_list, message_list = zip(*list3)

#Calculating the Ratio of each individual
ratio_list4 = []
for x in list3:
    ratio = float(x[1]) / float(total_distro_messages)
    ratio_list4.append(ratio)

#Rounding the ratios
list5 = []
for x in ratio_list4:
    
    y = round(x, 3)
    list5.append(y)

#Print on separte lines
for item in list3:
    print(' has sent a total of '.join(map(str, item)) + ' messages')\

conn.close()

#AWS variables and settings
aws_host = 'ratios.cfy6bgj8jv5o.us-west-2.rds.amazonaws.com'
aws_user = 'brohmo'
aws_password = 'aaaoooooohhhhhhhhhh'
aws_database = 'ratios'

#method to update the time table
def updatetime(cur, db):
    f = '%Y-%m-%d %H:%M:%S'
    dt = datetime.datetime.now().strftime(f)
    query ="UPDATE `ratios`.`updated` SET `updated` = '" + dt + "' WHERE `index` = '1';"
    print(dt)
    try:
        cur.execute(query)
        db.commit()
        print("Successfully updated time")
    except:
        db.rollback()
        print("Rolled back with an exception")

#method to update the ratio column within the table on AWS
def updateratios( cur, db ):
    for num in range(0, 11):
        rt = list5[num]
        nm = members[num]
        q1 = "UPDATE `ratios`.`ratios` SET `Ratio` = '" + str(rt) + "' WHERE `Name` = '" + nm + "';"
        try:
            cur.execute(q1)
            db.commit()
            print("Successfully..." + q1 )
        except:
            db.rollback()
            print("UN-successfully..." + q1 )

#method to update the message column within the table on AWS
def updatemessages(cur, db):
    for num in range(0, 11):
        rt = message_list[num]
        nm = members[num]
        q1 = "UPDATE `ratios`.`ratios` SET `Total` = '" + str(rt) + "' WHERE `Name` = '" + nm + "';"
        try:
            cur.execute(q1)
            db.commit()
            print("Successfully..." + q1 )
        except:
            db.rollback()
            print("UN-successfully..." + q1 )

#main method
db = mysql.connector.connect(host='ratios.cfy6bgj8jv5o.us-west-2.rds.amazonaws.com',database='ratios',user='brohmo',password='aaaoooooohhhhhhhhhh')
cur = db.cursor()
updateratios(cur, db)
updatemessages(cur, db)
updatetime(cur, db)

db.close()
