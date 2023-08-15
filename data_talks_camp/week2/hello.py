from prefect import flow, task

@task
def name(y: str):
    print(y)

@flow
def sth():
    result = 10
    print(result)

@flow
def greet():
    name('Marrie')
    sth()
    print('Hello I am trying out prefect')

if __name__ == '__main__':
    greet()