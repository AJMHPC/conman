import pickle

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

    # Loop the entries to be paged
    for n, entry in enumerate(entries):
        # Pickle it
        # Pickle the entries if necessary
        if as_pickle:
            entry = pickle.dumps(entry)
        # Save it to the page file & the number of bytes written so the point
        # at one entry ends and another starts is known
        journal.append(page.write(entry))


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
    # Loop over the data blocks, read them and de-pickle them if instructed
    results = [pickle.loads(page.read(i)) if unpickle else page.read(i)
               for i in journal]
    # Wipe the page file, its journal and reset the seek position
    journal.clear()
    page.truncate(0)
    page.seek(0)
    # Return the results
    return results
