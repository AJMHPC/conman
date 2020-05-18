if __name__ == '__main__':
    """Script will randomly decide to terminate itself if it receives a value
    larger than 0.9."""
    from conman.slave import Slave
    from conman.exceptions import ConmanKillSig
    with Slave('', 12346) as slave:
        result = None
        try:
            while True:
                job = slave(result)
                result = 'random result'
                # if the value is > 0.9 then terminate
                if job > 0.9:
                    print('Value is larger than 0.9; terminating')
                    exit()

        except ConmanKillSig:
            print('Received kill signal')
            exit()
