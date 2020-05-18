from ase.md.langevin import Langevin
from ase import units
from ase.calculators.emt import EMT

def smash_it(system):
    """Run a simple NVT molecular dynamics simulation for 0.1 ps at 200 k using
    the effective medium theory on a supplied system.

    Parameters
    ----------
    system : `ase.Atoms`
        The system that is to be simulated.

    Returns
    -------
    traj : `list` [`ase.Atoms`]
        A list of `ase.Atoms` objects comprising the system's trajectory.
    """
    # Tell the system to use the EMT calculator
    system.set_calculator(EMT())
    # Set up the NVT simulation using the Langevin thermostat
    dyn = Langevin(system, 0.01 * units.fs, 200 * units.kB, 0.002)
    # Create a list to hold the trajectory & a function to populate it
    traj = []
    def update_traj(a=system):
        traj.append(a.copy())
    # Attach the trajectory populator
    dyn.attach(update_traj, interval=10)
    # Run the molecular dynamics simulation from 10000 steps
    dyn.run(10000)
    # Return the trajectory
    return traj


if __name__ == '__main__':
    """Script takes a system, runs a sort MD simulation on it and returns the
    trajectory """
    from conman.slave import Slave
    from conman.exceptions import ConmanKillSig
    with Slave('', 12345) as slave:
        trajectory = None
        try:
            while True:
                # Return the trajectory of the system and fetch a new system
                system = slave(trajectory)
                # Run a short MD system and get the trajectory
                trajectory = smash_it(system)

        except ConmanKillSig:
            print('Received kill signal')
            exit()
