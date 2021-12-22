import subprocess

bashCommand = "node ../TweetsToPsql/insert.js output/replies.json"
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
print(output)