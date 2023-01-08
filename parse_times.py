import csv

time_list = []
with open('logs/times.csv', 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter='|', quotechar='|')
    for row in spamreader:
        time_list.append(list(row)[2])

time_list[-31:-1]
