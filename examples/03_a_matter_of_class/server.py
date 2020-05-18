
class TestClass:
    """A simple test class to exemplify the behaviours of classes within ConMan.

    Parameters
    ----------
    x : `int`
        The first magical number.
    y : `int`
        The second magical number.

    Properties
    ----------
    z : `int`, 'str'
        The product of x and y

    """
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.z = '?'

    def do_stuff(self):
        """Multiplies the ``x`` and ``y`` properties and sets the z property.
        """
        self.z = self.x * self.y

    def __str__(self):
        """Creates a string representing the class instance.
        """
        return f'{self.x} * {self.y} = {self.z}'


if __name__ == '__main__':
    """Constructs a class instance, sends it off and awaits a responce."""
    from conman.master import Master
    from numpy.random import randint
    with Master('', 12345) as master:
        master.mount(await_n=1)
        # Create a TestClass instance to send out
        my_class_out = TestClass(randint(0,100), randint(0,100))
        # Print out the class instance before "sending if off"
        print('Original Class Before Submission')
        print(f'\t{my_class_out}')
        master(jobs=[my_class_out], fetch=False)
        # Wait for the job to complete
        my_class_in = master.await_results()[0]
        # Print out the class after job submission
        print('Original Class After Submission')
        print(f'\t{my_class_out}')
        print('\t(Nothing changes you see)')
        # Print out the returned class
        print('Returned Class After Submission')
        print(f'\t{my_class_in}')
        print('\t(Returned class has been updated)')

