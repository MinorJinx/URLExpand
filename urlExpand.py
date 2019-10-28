import requests, csv
from queue import Queue
from threading import Thread

def readFile(filename):		# Opens input file, appends all items to jobs[], calls expand()
	file = open(filename)
	reader = csv.reader(file)
	for item in reader:
		jobs.append(item[0])
	file.close()
	
	print(str(len(jobs)) + " URLs found.\n")
	
	for i in range(30):		# Start threads, each one will process one job from the jobs[] queue
		t = Thread(target = expand, args = (queue,))
		t.setDaemon(True)
		t.start()
		
	for job in jobs:		# For each item in jobs[], put each into the queue in FIFO sequence
		queue.put(job)
	queue.join()			# Wait until all jobs are processed before quitting

def expand(queue):
	while True:
		try:
			url = queue.get()	# Retrieves item from queue
			response = requests.get(url)
			urlList.append(response.url)
			global count
			count += 1
			if count % 10000 == 0:
				print(count, response.url[:65] + (response.url[65:] and ' ...'))
			queue.task_done()
			
		except requests.HTTPError as e:
			print("HTTP Error 500")
			queue.task_done()

		except requests.ConnectionError:
			#print("HTTP Connection Error!")
			queue.task_done()
		
		except requests.Timeout as e:
			print("Timeout Error")
			queue.task_done()
		
		except Exception as e:
			print('CATCH ALL')
			print(e.__class__)

def saveOutput(url):
	with open(output, 'a', newline='') as file:
		writer = csv.writer(file)
		writer.writerow([url])

input = "test.csv"
output = "output.csv"
count = 0
jobs = []
urlList = []
queue = Queue()

readFile(input)

with open(output, 'a', newline='') as file:
	writer = csv.writer(file)
	for url in urlList:
		writer.writerow([url])
