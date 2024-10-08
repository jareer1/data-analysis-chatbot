# MYSQL Installation 

Installation and Setup of MySQL and MySQL Workbench
MySQL is a widely used relational database management system, and MySQL Workbench is a graphical tool for working with MySQL databases. Here's a detailed guide on how to install and set up MySQL and MySQL Workbench on macOS, Linux, and Windows.

## On macOS
### Installing MySQL
Download MySQL:

Visit the MySQL Community Downloads page. https://dev.mysql.com/downloads/mysql/
Select the macOS version (DMG Archive) and download it.
Install MySQL:

Open the downloaded DMG file and follow the on-screen instructions.
During the installation, you'll be prompted to set up a root password. Make sure to remember this password.


### Download MySQL Workbench:

Visit the MySQL Workbench Downloads page. https://dev.mysql.com/downloads/workbench/
Select the macOS version and download it.
Install MySQL Workbench:

Open the downloaded DMG file and drag MySQL Workbench to the Applications folder.


## On Linux (Ubuntu)

### Installing MySQL

Update Package Index:
sudo apt update

Install MySQL:
sudo apt install mysql-server

Secure MySQL Installation:
sudo mysql_secure_installation

Follow the prompts to set up the root password and secure your MySQL installation.
Start MySQL Service:
sudo systemctl start mysql

Optionally enable it to start on boot:
sudo systemctl enable mysql


Installing MySQL Workbench


### Download MySQL Workbench:

Visit the MySQL Workbench Downloads page. https://dev.mysql.com/downloads/workbench/
Select the Linux version and download the DEB file.

Install MySQL Workbench:
sudo dpkg -i mysql-workbench-community-*.deb
sudo apt-get -f install  # To fix any dependency issues


## On Windows

### Installing MySQL

Download MySQL Installer:

Visit the MySQL Community Downloads page. https://dev.mysql.com/downloads/mysql/
Download the MySQL Installer for Windows.

Install MySQL:
Run the downloaded installer and follow the setup instructions.
During the installation, select the "Developer Default" setup type for a complete MySQL installation.
Set up a root password when prompted.

Start MySQL Server:

The MySQL server should start automatically after installation. If not, you can start it via the MySQL Workbench or from the command line.


### Installing MySQL Workbench
Download MySQL Workbench:

The MySQL Workbench installer can be installed with the MySQL Installer. If not Visit the MySQL Workbench Downloads page. https://dev.mysql.com/downloads/workbench/
and download microsoft version.

Open MySQL Workbench:

Once installed, you can open MySQL Workbench from the Start menu.


## Connecting to MySQL Server using MySQL Workbench

Launch MySQL Workbench:

Open MySQL Workbench from your Applications (macOS), start menu (Windows), or application menu (Linux).
Create a New Connection:

Click on the "MySQL Connections" tab and then click the "+" button to create a new connection.
Enter the connection name, hostname (usually localhost), port (default is 3306), and the root username.
Click "Test Connection" and enter the root password you set during installation.
If the connection is successful, click "OK" to save it.
Manage Databases:

Once connected, you can manage your databases, run queries, and perform other database tasks using MySQL Workbench's graphical interface.


References:

- Install mysql -> For mac: brew install mysql or follow https://flaviocopes.com/mysql-how-to-install/
- Run: mysql.server start
- Install mysql-workbench -> https://www.mysql.com/products/workbench/
- Install mysql-connector -> pip install mysql-connector


# System Requirements for Open AI API Execution

Using the OpenAI API involves several considerations related to the system requirements and setup to ensure smooth and efficient operation. Below are the key aspects to consider:

### 1. Operating System
The OpenAI API can be accessed from any operating system that supports HTTP requests. Commonly used operating systems include:

Windows: Windows 10 or later is recommended.
macOS: macOS 10.15 (Catalina) or later.
Linux: Modern distributions such as Ubuntu 18.04 or later

### 2. Hardware Requirements
The hardware requirements are relatively minimal, as the actual processing happens on OpenAI's servers. However, for development and integration purposes, consider the following:

Processor: A modern multi-core processor 
Memory: At least 8GB of RAM is recommended, if you are handling large datasets or multiple applications simultaneously consider more.
Storage: Sufficient storage space for your development environment and any necessary dependencies. SSD storage is recommended for faster read/write operations.

### 3. Internet Connection
A stable and reasonably fast internet connection is essential, as interactions with the OpenAI API require sending and receiving data over the internet.

Speed: A minimum of 2-4 Mbps is recommended to handle API requests and responses efficiently.
Stability: A stable connection with low latency is crucial to avoid timeouts and ensure quick responses from the API.

### 4. API Key and Authentication
API Key: To use the OpenAI API, you need an API key, which you can obtain by signing up for an account on the OpenAI platform and creating an API key.








# Code Execution Instructions

## Python version 3.10

To create a virtual environment and install requirements in Python 3.10 on different operating systems, follow the instructions below:

### For Windows:

Open the Command Prompt by pressing Win + R, typing "cmd", and pressing Enter.

Change the directory to the desired location for your project:


`cd C:\path\to\project`

Create a new virtual environment using the venv module:


`python -m venv myenv`

Activate the virtual environment:

`myenv\Scripts\activate`


Install the project requirements using pip:

`pip install -r requirements.txt`

### For Linux/Mac:
Open a terminal.

Change the directory to the desired location for your project:

`cd /path/to/project`

Create a new virtual environment using the venv module:

`python3.10 -m venv myenv`


Activate the virtual environment:

`source myenv/bin/activate`

Install the project requirements using pip:

`pip install -r requirements.txt`

These instructions assume you have Python 3.10 installed and added to your system's PATH variable.

## Execution Instructions if Multiple Python Versions Installed

If you have multiple Python versions installed on your system, you can use the Python Launcher to create a virtual environment with Python 3.10. Specify the version using the -p or --python flag. Follow the instructions below:

For Windows:
Open the Command Prompt by pressing Win + R, typing "cmd", and pressing Enter.

Change the directory to the desired location for your project:

`cd C:\path\to\project`

Create a new virtual environment using the Python Launcher:

`py -3.10 -m venv myenv`

Note: Replace myenv with your desired virtual environment name.

Activate the virtual environment:

`
myenv\Scripts\activate
`

Install the project requirements using pip:

`pip install -r requirements.txt`


### For Linux/Mac:
Open a terminal.

Change the directory to the desired location for your project:

`cd /path/to/project
`
Create a new virtual environment using the Python Launcher:


`python3.10 -m venv myenv`


Note: Replace myenv with your desired virtual environment name.

Activate the virtual environment:

`source myenv/bin/activate`

Install the project requirements using pip:

`pip install -r requirements.txt`


By specifying the version using py -3.10 or python3.10, you can ensure that the virtual environment is created using Python 3.10 specifically, even if you have other Python versions installed.



## Run streamlit application

`streamlit run app.py`




