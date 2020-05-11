"""
This contains all of the custom exceptions
"""


class ConmanError(Exception):
    """The base exeption class upon which all conman exceptions are based.
    """
    pass


class ConmanKillSig(ConmanError):
    """Raised when the CONMAN_KILL signal is received.
    """
    pass


class ConmanIncompleteMessage(ConmanError):
    """Raised when an incomplete message has been received.
    """
    pass


class ConmanMaxSlaveLoss(ConmanError):
    """Raised when the maximum permitted slave casualty count has been breached."""
    pass
