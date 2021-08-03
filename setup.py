import setuptools

with open('README.md') as f:
    long_description = f.read()

requirements = []
with open('requirements.txt') as f:
    for line in f:
        requirements.append(line)

setuptools.setup(
    name = 'news2graph-jsp',
    version = '0.0.2',
    author = 'Jakob Speier',
    author_email = 'jakob.speier+n2g@gmail.com',
    description = 'Gathering, transformation and storage of news articles as graph representations.',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url='https://www.github.com/jotespeh/news2graph',
    packages=setuptools.find_packages(),
    python_requires='>=3.8',
    install_requires=requirements
)