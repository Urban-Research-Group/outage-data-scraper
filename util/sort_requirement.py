requirements_file = '../requirements.txt'

requirements = open(requirements_file, 'r')
content = requirements.read().splitlines()
content = list(set(content))
content.sort(key=lambda y: y.lower())
content = '\n'.join(content)

file = open(requirements_file, 'w')
file.write(content)
file.close()
