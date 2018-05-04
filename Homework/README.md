# Homework 1: (20 pts)

1. Form a team of at most 4 students (including yourself).
2. Identify a "Big Data" application and write a report with a detailed description of the following:
- Application domain
- Four V's of big data
- Big data-related problems (computing, storage, database, analytics, transfer, etc.)
- Existing solutions
- Solutions proposed by your team

Submission requirements: A zipped file that contains
- A PDF file of the report: 5 full pages including 5 - 10 references, single space, single column, 12 pts font, 1-inch margins (10 pts)
- A PPT file of <=15 slides for a 15-minute in-class presentation (10 pts)

# Homework 2: (10 pts)

Take the following steps to set up VM instances through AWS for later use:

- Create an Amazon account (if you don't have one yet): http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/AboutAWSAccounts.html
- Apply as a student for free credits: http://aws.amazon.com/education/awseducate
- Create and launch two basic Amazon EC2 instances using any Linux AMI of your choice
- Assign an appropriate security group (with appropriate firewall settings) to allow network traffic between your two instances

Note: Since NJIT is a member of AWS Educate, each student gets $100 credits upon request. But you'll have to wait several hours/days to receive your $100 credits.

Submission requirements: A zipped file that contains
- A snapshot of the Amazon instance management web interface showing the running state of each of your VM instances
- A snapshot of ssh-based connection between two of your VM instances

# Homework 3: (30 pts)

On one of the VM instances you created in HW2, do the following:
- Download, install, and run the latest release of Apache Hadoop in a non-distributed or local mode (standalone): http://hadoop.apache.org/releases.html
- Develop and test a MapReduce-based approach in your Hadoop system to find all the missing Poker cards.

Submission requirements: A zipped file that contains
- A text file that contains a random number (<52) of different Poker cards (each card is represented by both its rank and suit)
- A text file that contains all the missing Poker cards identified by your MapReduce solution
- The java programs of your MapReduce solution

# Homework 4: (60 pts)

In this assignment, you will explore a set of 100,000 Wikipedia documents: 100KWikiText.txt, in which each line consists of the plain text extracted from an individual Wikipedia document. On the AWS VM instances you created in HW2, do the following:
- Configure and run the latest release of Apache Hadoop in a pseudo-distributed mode.
- Develop a MapReduce-based approach in your Hadoop system to compute the relative frequencies of each word that occurs in all the documents in 100KWikiText.txt, and output the top 100 word pairs sorted in a decreasing order of relative frequency. Note that the relative frequency (RF) of word B given word A is defined as follows: where count(A,B) is the number of times A and B co-occur in the entire document collection, and count(A) is the number of times A occurs with anything else. Intuitively, given a document collection, the relative frequency captures the proportion of time the word B appears in the same document as A.
- Repeat the above steps using at least 2 VM instances in your Hadoop system running in a fully-distributed mode.

Submission requirements: All the following files must be submitted in a zipped file:
- A commands.txt text file that lists all the commands you used to run your code and produce the required results in both pseudo and fully distributed modes
- A top100.txt text file that stores the final results (only the top 100 word pairs sorted in a decreasing order of relative frequency)
- The source code of your MapReduce solution (including the JAR file)
- An algorithm.txt text file that describes the algorithm you used to solve the problem
- A settings.txt text file that describes:
  i) the input/output format in each Hadoop task, i.e., the keys for the mappers and reducers
  ii) the Hadoop cluster settings you used, i.e., number of VM instances, number of mappers and reducers, etc.
  iii) the running time for your MapReduce approach in both pseudo and fully distributed modes
