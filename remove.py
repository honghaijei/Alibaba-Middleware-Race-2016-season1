lines = []
with open('log_haijie.txt', 'r') as f:
    lines = [i for i in f.readlines() if 'SplitSentence' in i or 'WordCount' in i or 'RaceSentenceSpout' in i]
with open('log_haijie2.txt', 'w') as f:
	f.writelines(lines)