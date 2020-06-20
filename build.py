import urllib.request
import subprocess


# build password data

word_list_url = "https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-no-swears.txt"

response = urllib.request.urlopen(word_list_url)
words_txt = response.read().decode("utf8")

words = []
for wline in words_txt.split("\n"):
    if len(wline) < 4:
        continue
    else:
        words.append(wline.strip())

with open("lcserver/word_list.py", "w") as ff:
    ff.write(
        "# This list is compiled from https://github.com/first20hours/google-10000-english\n"
    )
    ff.write("WORD_LIST = ")
    ff.write(repr(words))

subprocess.run(["black", "lcserver/word_list.py"])
