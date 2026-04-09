export const policies = [
  // 💰 Finance / Loan
  {
    id: 'fin-1',
    category: 'Finance',
    title: '신생아 특례 디딤돌 대출',
    summary: '최저 1.6% 금리로 최대 5억원 지원',
    description: '출산 가구의 주거 안정을 위해 저금리로 주택 구입 자금을 대출해주는 정부 지원 사업입니다.',
    criteria: '소득 1.3억원 이하, 자산 4.69억원 이하 가구',
    benefit: '금리 1.6% ~ 3.3%, 최대 5억원 한도',
    applyMethod: '주택도시기금 기금e든든 홈페이지',
    tags: ['출산가구', '저금리', '최대5억'],
    // Filter Metadata
    incomeLimit: 13000,
    assetLimit: 46900,
    requiresKids: true,
    maritalStatus: 'All'
  },
  {
    id: 'fin-2',
    category: 'Finance',
    title: '버팀목 전세자금대출 (청년/신혼)',
    summary: '신혼부부 전용 최저 1.5% 전세 대출',
    description: '청년 및 신혼부부의 주거비 부담을 줄여주기 위해 전세 자금을 저금리로 대출해주는 정책입니다.',
    criteria: '연소득 7,500만원 이하 (신혼 8,500만원 이하)',
    benefit: '금리 1.5% ~ 2.4%, 보증금의 80% 이내 지원',
    applyMethod: '수탁은행 (우리, 국민, 기업 등) 및 기금e든든',
    tags: ['신혼부부', '청년전용', '전세자금'],
    // Filter Metadata
    incomeLimit: 8500,
    assetLimit: 34500,
    ageMax: 34,
    maritalStatus: 'All'
  },

  // 🏠 Housing
  {
    id: 'hou-1',
    category: 'Housing',
    title: '청년/신혼부부 월세 지원',
    summary: '매월 최대 20만원, 최장 12개월 지원',
    description: '소득이 낮은 청년 및 신혼부부의 주거 안정을 위해 월세를 현금으로 지원하는 제도입니다.',
    criteria: '각 지자체별 소득 및 거주 기준 상이',
    benefit: '매월 최대 20만원 지원 (공공임대 거주자 제외)',
    applyMethod: '복지로(bokjiro.go.kr) 또는 주민센터 방문',
    tags: ['월세지원', '현금지원', '지자체협력'],
    // Filter Metadata
    incomeLimit: 5000,
    ageMax: 34,
    maritalStatus: 'All'
  },
  {
    id: 'hou-2',
    category: 'Housing',
    title: '청년안심주택 (구 역세권 청년주택)',
    summary: '역세권 고품질 임대주택 우선 공급',
    description: '교통이 편리한 역세권에 시세 대비 저렴한 가격으로 청년 및 신혼부부에게 주택을 공급합니다.',
    criteria: '무주택 청년/신혼부부, 소득 및 자산 기준 충족자',
    benefit: '주변 시세의 30%~95% 수준 임대료',
    applyMethod: 'SH공사 청약센터 또는 민간사업자 홈페이지',
    tags: ['역세권', '임대주택', '입주우선권'],
    // Filter Metadata
    incomeLimit: 7000,
    ageMax: 39,
    maritalStatus: 'All'
  },

  // 🎁 Welfare
  {
    id: 'wel-1',
    category: 'Welfare',
    title: '부모급여 (0~1세 아동)',
    summary: '0세 월 100만원, 1세 월 50만원 현금 지급',
    description: '아이를 키우는 부모의 경제적 부담을 덜어주기 위해 아동 연령에 따라 현금을 지급합니다.',
    criteria: '0~23개월 아동을 양육하는 부모',
    benefit: '0세 월 100만원 / 1세 월 50만원 (어린이집 이용 시 차감)',
    applyMethod: '정부24 또는 읍면동 행정복지센터',
    tags: ['부모급여', '현금복지', '양육지원'],
    // Filter Metadata
    requiresKids: true,
    maritalStatus: 'Married'
  },
  {
    id: 'wel-2',
    category: 'Welfare',
    title: '청년 국가기술자격 시험 응시료 지원',
    summary: '연 3회 응시료의 50% 지원 (최대 1만원)',
    description: '청년의 자기계발과 취업 준비를 돕기 위해 국가기술자격 시험 응시료를 감면해줍니다.',
    criteria: '19세~34세 이하 청년 (연령 제한 지자체별 상이)',
    benefit: '응시료 50% 감면 (1인당 연 3회 제한)',
    applyMethod: 'Q-Net 홈페이지 접수 시 자동 적용',
    tags: ['시험응시료', '취업준비', '자기계발'],
    // Filter Metadata
    ageMin: 19,
    ageMax: 34,
    maritalStatus: 'All'
  }
];
