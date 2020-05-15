from sys import argv

from conman.master import Master
from conman.slave import Slave


def test_slave_echo(address):
    print('Slave is booting up')
    with Slave(*address) as slave:
        print('Slave has booted')
        first_job = slave(None)
        print(f'First job is: {first_job}')
        second_job = slave('First_Response')
        print(f'Second job is: {second_job}')
        slave('Second_Response')
        print('Slave is exiting')
    print('Slave has exited')

def test_master_echo(address):
    print('Master is booting up')
    with Master(*address) as master:
        print('Master has booted')
        print('Master is attempting to mount 1 slave')
        master.mount(await_n=1)
        print('Sending jobs')
        master(['First_Message', 'Second_Message'], fetch=False)
        print('Awaiting results')
        results = master.await_results()
        print('The Results Are:')
        for r in results:
            print(f'\t{r}')
        print('Master is exiting')
    print('Master has exited')



if __name__ == '__main__':
    address = ('', 28337)
    if argv[1] == 'm':
        test_master_echo(address)
    elif argv[1] == 's':
        test_slave_echo(address)
    else:
        print(f'"{argv[1]}" is an unknown option')