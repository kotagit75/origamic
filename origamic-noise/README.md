# origamic-noise

`origamic-noise` provides an encryption layer for the Origamic P2P network using the Noise protocol framework.
It acts as a transport decorator, taking an underlying stream (e.g., TCP) and upgrading it to a secure, encrypted stream.

> [!CAUTION]
> **Experimental Stage**
> The current implementation uses a Trust-On-First-Use (TOFU) verifier by default, meaning it accepts all unknown remote static keys. It is currently unverified whether this obfuscation and encryption layer functions as expected in a hostile environment. Use it with caution and at your own risk.

## Usage
Wrap your existing factory (like `TcpFactory`) with the Noise factory to transparently encrypt the transport layer before passing it to the Origamic overlay.

## License
MIT License
