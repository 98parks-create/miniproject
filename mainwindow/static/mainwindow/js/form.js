let currentUserData = null;
        let matchedPolicies = [];

        function setView(viewId) {
            ['hero', 'form', 'matching', 'result'].forEach(v => {
                const el = document.getElementById('view-' + v);
                if(el) el.classList.add('hidden');
            });
            document.getElementById('view-' + viewId).classList.remove('hidden');
            window.scrollTo(0,0);
        }

        function nextStep(step) {
            [1, 2, 3].forEach(s => {
                document.getElementById('step-' + s).classList.remove('active');
                document.getElementById('dot-' + s).classList.remove('active');
            });
            document.getElementById('step-' + step).classList.add('active');
            document.getElementById('dot-' + step).classList.add('active');
        }

        async function handleMatch() {
            setView('matching');
            
            const userData = {
                age: parseInt(document.getElementById('inp-age').value),
                income: parseInt(document.getElementById('inp-income').value),
                region: document.getElementById('inp-region').value,
                marital: document.getElementById('inp-marital').value,
                nonHomeowner: document.getElementById('inp-non-homeowner').checked,
                lowIncome: document.getElementById('inp-low-income').checked
            };
            currentUserData = userData;

            try {
                // Fetch Policies (POST로 진단 데이터 전송)
                const res = await fetch('/chatbot/api/policies/', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json', 'X-CSRFToken': '{{ csrf_token }}'},
                    body: JSON.stringify({ user_data: userData })
                });
                const data = await res.json();
                const allPolicies = data.policies || [];
                
                // Score locally for instant feedback
                matchedPolicies = allPolicies.map(p => {
                    let score = p.score || 70; // Use server score if available
                    if(p.ageMax && userData.age > p.ageMax) score *= 0.5;
                    if(p.incomeLimit && userData.income > p.incomeLimit) score *= 0.5;
                    return {...p, score};
                }).sort((a,b) => b.score - a.score);

                renderResults();
                await fetchAIReport(userData, matchedPolicies.slice(0, 5));
                setView('result');
            } catch(e) {
                console.error(e);
                setView('form');
                alert("분석 중 오류가 발생했습니다.");
            }
        }

        function renderResults() {
            const grid = document.getElementById('policy-results-grid');
            grid.innerHTML = matchedPolicies.slice(0, 6).map(p => `
                <div class="policy-card-result">
                    <div class="score-badge">${Math.round(p.score)}%</div>
                    <h4>${p.title}</h4>
                    <p>${p.summary}</p>
                </div>
            `).join('');
            
            document.getElementById('policy-count').innerText = matchedPolicies.length + '건';
            document.getElementById('gauge-text').innerText = Math.round(matchedPolicies[0]?.score || 0) + '%';
        }

        async function fetchAIReport(userData, topMatches) {
            const box = document.getElementById('ai-report-content');
            try {
                const res = await fetch('/chatbot/api/ai-report/', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json', 'X-CSRFToken': '{{ csrf_token }}'},
                    body: JSON.stringify({ user_data: userData, top_matches: topMatches })
                });
                const data = await res.json();
                box.innerHTML = `<p class="report-paragraph">${data.report}</p>`;
            } catch(e) { 
                box.innerText = "보고서 생성 중 세션이 만료되었습니다. 아래 리스트를 참고해 주세요."; 
            }
        }

        // Chatbot Functions
        function toggleChatbot() { document.getElementById('chatbot-modal').classList.toggle('hidden'); }
        
        async function handleChatSubmit(e) {
            e.preventDefault();
            const input = document.getElementById('chat-input');
            const msg = input.value.trim();
            if(!msg) return;

            appendChat('user', msg);
            input.value = '';

            try {
                const response = await fetch('/chatbot/api/chat/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value
                    },
                    body: JSON.stringify({ message: msg, user_data: currentUserData })
                });
                const data = await response.json();
                appendChat('bot', data.reply);
            } catch(e) {
                appendChat('bot', "오류가 발생했습니다.");
            }
        }

        function appendChat(role, text) {
            const box = document.getElementById('chat-messages');
            const div = document.createElement('div');
            div.className = `chat-bubble chat-${role}`;
            div.innerHTML = text.replace(/\n/g, '<br/>');
            box.appendChild(div);
            box.scrollTop = box.scrollHeight;
        }