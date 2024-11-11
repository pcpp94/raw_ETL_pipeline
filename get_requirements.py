import os
import glob

BASE_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))

requirements = glob.glob(os.path.join(BASE_DIR, "**", 'requirements.txt'))
overall_requirements = []

for requirement in requirements:
    with open(requirement, 'r') as txt:
        packages = txt.readlines()
        packages = [x.replace('\n', '') for x in packages]
    overall_requirements.extend(packages)

overall_requirements = list(set(overall_requirements))
overall_requirements.sort()

with open(os.path.join(BASE_DIR,"requirements.txt"), 'w') as req:
    for item in overall_requirements:
        req.write(f"{item}\n")

