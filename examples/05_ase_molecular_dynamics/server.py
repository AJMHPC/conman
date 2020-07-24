import numpy as np
from numpy.linalg import norm
from ase.collections import g2


# Strip out all system from g2 molecule set that contain elements other than H,
# C, N & O as only said elements can be meddled by ase's EMT calculator
def ok(mol):
    permitted = [1, 6, 7, 8]
    return len([e for e in mol.get_atomic_numbers() if e not in permitted]) == 0


g2 = [mol for mol in g2 if ok(mol)]


def system_to_smash():
    """Constructs and returns a cell with two random molecules about to collide.

    Returnsa
    -------
    system : `ase.Atoms`
        A 10x10x10 cell with two randomly selected molecules spaced out along
        the x-axis. Periodic boundary conditions are enacted and the molecules
        have a large predefined velocity with will cause them to destructively
        collide.
    """
    # Select two molecules at random
    mol_1, mol_2 = np.random.choice(g2, 2, replace=False)
    # Calculate centers of mass
    com_1, com_2 = mol_1.get_center_of_mass(), mol_2.get_center_of_mass()
    # Calculate distance from com to outermost atom
    r_1 = max(norm(mol_1.positions - com_1, axis=1))
    r_2 = max(norm(mol_2.positions - com_2, axis=1))
    # Separate the molecules along the x axis
    mol_1.positions -= (com_1 + r_1 + 0.5) * np.array([1., 0, 0])
    mol_2.positions += (-com_2 + r_2 + 0.5) * np.array([1., 0, 0])
    # Set them off on a collision trajectory
    mol_1.set_velocities((np.array([0.2, 0, 0],) * len(mol_1)))
    mol_2.set_velocities((np.array([-0.2, 0, 0], ) * len(mol_2)))
    # Combine the two systems
    system = mol_1 + mol_2
    # Turn on periodic boundary conditions
    system.pbc = True
    # Set the cell size (give some space on the ends of the cell)
    system.cell = np.array([10.0, 10.0, 10.0])
    # Center the systems in the cell
    system.center()
    # Return the system
    return system

if __name__ == '__main__':
    """Sends workers systems to run MD simulations on, concatenates the results
    and saves them to a file."""
    from conman.coordinator import Coordinator
    from ase.io import write
    with Coordinator('', 12345, max_worker_loss=0) as coordinator:
        coordinator.mount(await_n=2)
        # Submit a number of jobs equal to double the number of workers
        jobs = [system_to_smash() for _ in range(len(coordinator.workers) * 2)]
        coordinator(jobs, fetch=False)
        # Wait for the results to come back
        results = coordinator.await_results()
    # Flaten the results list into a single trajectory
    trajectory = [i for j in results for i in j]
    # Save it to a xyz file
    write('smashing.xyz', trajectory)
    # To view: type "ase gui smashing.xyz" into the console

