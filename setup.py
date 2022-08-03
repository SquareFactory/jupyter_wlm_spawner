import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jupyter-wlm-spawner",
    version="0.0.3",
    author="Marc Nguyen, Dmitry Chirikov",
    author_email="marc@squarefactory.io, dmitry@chirikov.nl",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "jupyter-client>=7.3,<8.0",
        "pycryptodome>=3.15,<4.0",
    ],
    url="https://github.com/SquareFactory/jupyter_wlm_spawner",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
