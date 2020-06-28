import sqlite3

RESET = True

emails_loc = "emails.db"
emails_connection = sqlite3.connect(emails_loc)
emails_cursor = emails_connection.cursor()

output_loc = "output.db"
output_connection = sqlite3.connect(output_loc)
output_cursor = output_connection.cursor()
output_cursor.execute("CREATE TABLE IF NOT EXISTS users (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, pseudo VARCHAR(20) NOT NULL, email VARCHAR(200), password VARCHAR(200) NOT NULL, correct_password BOOLEAN NOT NULL, ip VARCHAR(50), money INTEGER)")
output_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS `ix` ON `users` (pseudo, ifnull(email,'Null'), password, ifnull(ip,'Null'))")
if RESET:
    output_cursor.execute("DELETE FROM users")
output_connection.commit()

file = open("input.csv", "r")
data = file.readlines()

total = len(data)
current = 0
entries = 0
separator = '","'

def WriteToDB(pseudo, email, password, money, password_correct, ip):
    global entries
    try:
        output_cursor.execute("INSERT INTO users (pseudo, email, password, correct_password, ip, money) VALUES (?, ?, ?, ?, ?, ?)", (pseudo, email, password, password_correct, ip, money))
        output_connection.commit()
        entries += 1
    except sqlite3.IntegrityError:
        return

for line in data:
    clusters = line.split(separator)
    #print(clusters)
    if len(clusters) > 1:
        current += 1
        words = clusters[3].split()
        if " login " in clusters[3]:
            try:
                pseudo = words[2]
                password = words[4]
            except IndexError:
                print("Error while processing data n°"+str(current)+". Player or Password not found. Line details: "+line+"  Skipping...")
            email_result = emails_cursor.execute("SELECT email,ip,money FROM users WHERE pseudo = ?", (pseudo,))
            results = [None,None,None]
            for row in email_result:
                results = row
            WriteToDB(pseudo, results[0], password, results[2], True, results[1])

        elif " mauvais mdp " in clusters[3]:
            try:
                pseudo = words[2]
                password = words[5]
            except IndexError:
                print("Error while processing data n°"+str(current)+". Player or Password not found. Line details: "+line+"  Skipping...")
            email_result = emails_cursor.execute("SELECT email,ip,money FROM users WHERE pseudo = ?", (pseudo,))
            results = [None,None,None]
            for row in email_result:
                results = row
            WriteToDB(pseudo, results[0], password, results[2], False, results[1])

        if current%1000 == 0:
            print("Processing data... "+str(current)+"/"+str(total)," ("+str(round(current/total*100,2))+"%). "+str(entries)+" entries in database so far...")

print("Complete! "+str(entries)+" total entries in database")

output_cursor.close()
output_connection.close()

emails_cursor.close()
emails_connection.close()
