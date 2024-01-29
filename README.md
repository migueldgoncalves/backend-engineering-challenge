# Translation Event Processor
### Unbabel Backend Engineering Challenge January 2024

The original repository from Unbabel with the challenge instructions can be found [here](https://github.com/Unbabel/backend-engineering-challenge). It was forked on January 24th, 2024.

Developed and tested in Python 3.9, on Windows 10.

### How to run and test the code

#### Input file

Before running the application, the input file must be created. It can be placed in an arbitrary location inside the filesystem, with an arbitrary name.

Placing the file in the root folder of the application allows to provide the filename rather than the absolute path in the startup command of the application.

#### Output file

The output file will be named ``output.txt``, and it will appear in the current directory when the application is launched.

For this reason, it is recommended to run the application with the current directory set to the **root folder of the app**.

#### Steps

1. Clone this repository, or download and unzip the code
2. With a terminal, such as Command Prompt, access the root folder of the application
3. Execute the file ``translation_event_processor.py``, the entry point of the application

An example command is the following:
``C:\Users\Username\AppData\Local\Programs\Python\Python39\python.exe translation_event_processor.py --input-file events.json --window-size 10``

Where:
- ``C:\Users\Username\AppData\Local\Programs\Python\Python39\python.exe`` is the absolute path of the Python executable. Windows 10 seems to require the full path to the Python executable.
- ``translation_event_processor.py`` is the name of the Python module to execute. If running the application inside its root folder, only the filename is required, and not its absolute path
- ``--input-file`` is the name of the flag with the input filename. It is not used by the application, it just needs to be a non-empty string
- ``events.json`` is the name of the input file. If running the application inside its root folder and if the file is also inside the folder, only the filename is required, and not its absolute path
- ``--window-size`` is the name of the flag with the desired window size. It is not used by the application, it just needs to be a non-empty string
- ``10`` is the desired window size in minutes. This parameter will have a direct impact in the output produced

Execution is expected to take less than 1 s, with no further need for input from the user.

It is possible to execute the application several times in a row without the need to manually delete the ``output.txt`` file, as the application does this automatically.

#### Automated unit testing

A unit test was added, ensuring that the sample input provided by Unbabel leads to the expected output.

To run it, access the root folder of the application. Then, run the following command:

``C:\Users\Username\AppData\Local\Programs\Python\Python39\python.exe -m unittest test\test_integration.py``

Where:

- ``C:\Users\Username\AppData\Local\Programs\Python\Python39\python.exe`` is the absolute path of the Python executable
- ``-m unittest`` tells Python to run the module ``unittest``
- ``test\test_integration.py`` is the relative path of the module with the unit test

Test execution is expected to take around 0.5 s.