import pickle

"""
TODO:
    - Read the whole page file at once and then split up after the fact
        to speed up the function.
"""

def save_to_page(entries, page, journal, as_pickle=True):
    """Saves data to a temporary page file. Primarily used to 1) stash
    pre-fetched results retried by background processes in an effort
    to free up slaves, and 2) save unsent jobs prior to transmission when
    no slave are available. Data is save to a page file rather than in
    memory as some results/jobs can be large enough to cause memory issues.

    Parameters
    ----------
    entries : `list` [`serialisable`]
        List of entities that are to be saved to the page file.
    page : `TemporaryFile`
        Temporary file in which to page the ``entries``.
    journal : `list` [`int`]
        List to which entry lengths are to be saved.
    as_pickle: 'bool', optional
        Used to specify if the entities should be pickled prior to paging.
        [DEFAULT=True]
    """
    # Pickle the entries if necessary
    if as_pickle:
        entries = [pickle.dumps(i) for i in entries]
    # Save the number of bytes of each entry to the journal so that the point at
    # which one entry ends and another starts is known.
    journal.extend([len(entry) for entry in entries])
    # Write the entries to the page file (all are written at once for efficiency)
    page.write(b''.join(entries))


def load_from_page(page, journal, unpickle=True):
    """Loads data from the a page file.

    page : `TemporaryFile`
        Temporary page file from which to load ``entries``.
    journal : `list` [`int`]
        List from which entry lengths are to be read.
    unpickle : `bool`, optional
        Indicates if the data should be unpicked prior to returning.
        [DEFAULT=True]

    Returns
    -------
    data : `list`
        List of data entries read from page file.

    Notes
    -----
    This will purge the page file after loading.
    """
    # Seek to the start of the page file
    page.seek(0)
    # Loop over and read in the data blocks
    results = [page.read(i) for i in journal]
    # De-pickle them if instructed (done in different loop for speed)
    if unpickle:
        results = [pickle.loads(i) for i in results]
    # Wipe the page file, its journal and reset the seek position back to the start
    journal.clear()
    page.truncate(0)
    page.seek(0)
    # Return the results
    return results
