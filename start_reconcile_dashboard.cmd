@echo off
cd /d "C:\Users\Administrator\DevProjs\OdooAPIProj\ApdiRepSysDb"
python -m streamlit run src/reconcile_dashboard.py --server.headless true --server.port 8501
