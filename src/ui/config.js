/* Laufzeitkonfiguration der UI.
 *
 * Diese Datei wird im Container beim Start aus der Umgebung neu geschrieben
 * (SCRUM-91), damit dasselbe Image lokal und im Cluster laeuft, ohne dass die
 * API-Adresse im Bundle steht.
 *
 * Leer lassen heisst: gleiche Herkunft wie die UI. Im Cluster stimmt das,
 * weil Traefik /api auf den Serving-Service leitet. Lokal, wenn die API auf
 * einem anderen Port laeuft, hier die Basis eintragen:
 *   window.APP_CONFIG = { apiBase: "http://localhost:8000" };
 */
window.APP_CONFIG = { apiBase: "" };
