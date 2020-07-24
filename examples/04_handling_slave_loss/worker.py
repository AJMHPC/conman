if __name__ == '__main__':
    """Script will randomly decide to terminate itself if it receives a value
    larger than 0.9."""
    from conman.worker import Worker
    from conman.exceptions import ConmanKillSig
    with Worker('', 12346) as worker:
        result = None
        try:
            while True:
                job = worker(result)
                result = 'random result'
                # if the value is > 0.9 then terminate
                if job > 0.9:
                    print('Value is larger than 0.9; terminating')
                    exit()

        except ConmanKillSig:
            print('Received kill signal')
            exit()
