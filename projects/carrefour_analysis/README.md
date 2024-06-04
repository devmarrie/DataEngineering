## Analysing the carrefour webste.
Helping customers save money and make well informed decisions when purchacing from the courefour website.

**Benefits of Using Selenium:**

   - JavaScript Execution: Selenium can handle dynamic content loaded via JavaScript, which is a limitation for the requests module.
    - Interactive Navigation: Selenium can interact with web elements, such as clicking pagination buttons if URLs are not sufficient.
    - Simulating Real Browsers: By using browser automation, it mimics human behavior, reducing the likelihood of being blocked.

This approach ensures you accurately scrape all data, including dynamically loaded content, and handle pagination effectively.

Check the selenium docs for more details here is the basic code:

```
from selenium import webdriver

# initialise the webdriver
driver = webdriver.Chrome()

# Fetch the desired webpage
driver.get("http://selenium.dev")

# close the driver
driver.quit()

```

Version 124.0.6367.201 (Official Build) (64-bit)   

### Convert a notebook to a python script
jupyter nbconvert --to script your_notebook.ipynb --output my_script.py

Here are the dependencies you need for integrating Spark with AWS S3, along with their versions compatible with Hadoop 3.3.2:

Jar packages for hadoop-aws https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.2 
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar

SDK - https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.11.1026
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

software.amazon.awssdk.transfer.s3.progress.TransferListener - wget https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.25.64/s3-transfer-manager-2.25.64.jar

wget https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/2.25.64/aws-core-2.25.64.jar