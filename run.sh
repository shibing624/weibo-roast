ps -ef | grep "st.py" | grep -v "grep" | awk '{print $2}' | xargs kill -9
nohup /home/xuming/anaconda3/envs/chatpilot/bin/streamlit run st.py --server.address=0.0.0.0 1>>st.log 2>>st.log &