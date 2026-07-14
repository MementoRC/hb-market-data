"""hb_compat: drop-in compatibility shims permitted to import hummingbot.

Modules in this subpackage are the ONLY place in market_data allowed to
import from hummingbot; everything else in the package stays hummingbot-free
(ADR 0001 boundary-debt cleanup, Group A1).
"""
