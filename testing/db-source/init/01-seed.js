// Seeds two databases for "Acme Corp" with five collections each.
// Runs once on first container start (mongo /docker-entrypoint-initdb.d).

const now = new Date();
const daysAgo = (n) => new Date(now.getTime() - n * 24 * 60 * 60 * 1000);
const money = (v) => Decimal128.fromString(v.toFixed(2));

// ─────────────────────────────────────────────────────────────
// DB 1: acme_crm — customer-facing data
// ─────────────────────────────────────────────────────────────
const crm = db.getSiblingDB('acme_crm');

crm.users.insertMany([
  { _id: ObjectId(), username: 'alice.nguyen',   email: 'alice@acme.test',   fullName: 'Alice Nguyen',    role: 'admin',     active: true,  createdAt: daysAgo(420), lastLoginAt: daysAgo(1),  prefs: { theme: 'dark',  tz: 'America/Los_Angeles' } },
  { _id: ObjectId(), username: 'bob.hart',       email: 'bob@acme.test',     fullName: 'Bob Hart',        role: 'sales',     active: true,  createdAt: daysAgo(380), lastLoginAt: daysAgo(2),  prefs: { theme: 'light', tz: 'America/New_York' } },
  { _id: ObjectId(), username: 'carmen.diaz',    email: 'carmen@acme.test',  fullName: 'Carmen Díaz',     role: 'sales',     active: true,  createdAt: daysAgo(310), lastLoginAt: daysAgo(5),  prefs: { theme: 'dark',  tz: 'Europe/Madrid' } },
  { _id: ObjectId(), username: 'dmitri.ivanov',  email: 'dmitri@acme.test',  fullName: 'Dmitri Ivanov',   role: 'support',   active: true,  createdAt: daysAgo(260), lastLoginAt: daysAgo(0),  prefs: { theme: 'dark',  tz: 'Europe/Berlin' } },
  { _id: ObjectId(), username: 'ella.okafor',    email: 'ella@acme.test',    fullName: 'Ella Okafor',     role: 'manager',   active: true,  createdAt: daysAgo(200), lastLoginAt: daysAgo(3),  prefs: { theme: 'light', tz: 'Africa/Lagos' } },
  { _id: ObjectId(), username: 'frank.mueller',  email: 'frank@acme.test',   fullName: 'Frank Müller',    role: 'sales',     active: false, createdAt: daysAgo(150), lastLoginAt: daysAgo(90), prefs: { theme: 'light', tz: 'Europe/Zurich' } },
  { _id: ObjectId(), username: 'gita.sharma',    email: 'gita@acme.test',    fullName: 'Gita Sharma',     role: 'analyst',   active: true,  createdAt: daysAgo(120), lastLoginAt: daysAgo(4),  prefs: { theme: 'dark',  tz: 'Asia/Kolkata' } },
  { _id: ObjectId(), username: 'hiro.tanaka',    email: 'hiro@acme.test',    fullName: 'Hiro Tanaka',     role: 'support',   active: true,  createdAt: daysAgo(95),  lastLoginAt: daysAgo(1),  prefs: { theme: 'dark',  tz: 'Asia/Tokyo' } },
  { _id: ObjectId(), username: 'iris.kovacs',    email: 'iris@acme.test',    fullName: 'Iris Kovács',     role: 'sales',     active: true,  createdAt: daysAgo(60),  lastLoginAt: daysAgo(0),  prefs: { theme: 'light', tz: 'Europe/Budapest' } },
  { _id: ObjectId(), username: 'juan.reyes',     email: 'juan@acme.test',    fullName: 'Juan Reyes',      role: 'manager',   active: true,  createdAt: daysAgo(30),  lastLoginAt: daysAgo(0),  prefs: { theme: 'dark',  tz: 'America/Mexico_City' } }
]);
crm.users.createIndex({ username: 1 }, { unique: true });
crm.users.createIndex({ email: 1 },    { unique: true });
crm.users.createIndex({ role: 1, active: 1 });

const customerDocs = [
  { name: 'Globex Corporation',   industry: 'Manufacturing', tier: 'enterprise', arr: money(480000), employees: 5200, country: 'US' },
  { name: 'Initech',              industry: 'Software',      tier: 'mid',        arr: money(72000),  employees: 180,  country: 'US' },
  { name: 'Umbrella Pharma',      industry: 'Pharma',        tier: 'enterprise', arr: money(910000), employees: 12000,country: 'CH' },
  { name: 'Hooli Labs',           industry: 'Software',      tier: 'enterprise', arr: money(650000), employees: 3400, country: 'US' },
  { name: 'Stark Industries',     industry: 'Defense',       tier: 'enterprise', arr: money(1200000),employees: 8900, country: 'US' },
  { name: 'Wayne Enterprises',    industry: 'Conglomerate',  tier: 'enterprise', arr: money(2100000),employees: 15000,country: 'US' },
  { name: 'Soylent Foods',        industry: 'Food',          tier: 'mid',        arr: money(96000),  employees: 420,  country: 'CA' },
  { name: 'Massive Dynamic',      industry: 'Research',      tier: 'enterprise', arr: money(540000), employees: 2100, country: 'US' },
  { name: 'Cyberdyne Systems',    industry: 'Robotics',      tier: 'mid',        arr: money(145000), employees: 680,  country: 'US' },
  { name: 'Tyrell Corp',          industry: 'Biotech',       tier: 'enterprise', arr: money(780000), employees: 4400, country: 'US' },
  { name: 'Oscorp',               industry: 'Chemicals',     tier: 'mid',        arr: money(88000),  employees: 310,  country: 'US' },
  { name: 'Weyland-Yutani',       industry: 'Aerospace',     tier: 'enterprise', arr: money(1350000),employees: 9800, country: 'GB' },
  { name: 'Pied Piper',           industry: 'Software',      tier: 'smb',        arr: money(24000),  employees: 42,   country: 'US' },
  { name: 'Dunder Mifflin',       industry: 'Paper',         tier: 'smb',        arr: money(18000),  employees: 90,   country: 'US' },
  { name: 'Vehement Capital',     industry: 'Finance',       tier: 'mid',        arr: money(210000), employees: 260,  country: 'UK' }
];
const customers = customerDocs.map((c, i) => ({
  _id: ObjectId(),
  ...c,
  status: i % 7 === 0 ? 'churned' : 'active',
  tags: i % 2 === 0 ? ['strategic', 'renewal'] : ['standard'],
  billingAddress: { line1: `${100 + i} Market St`, city: 'San Francisco', region: 'CA', postal: '9410' + (i % 10), country: c.country },
  createdAt: daysAgo(700 - i * 20),
  updatedAt: daysAgo(i)
}));
crm.customers.insertMany(customers);
crm.customers.createIndex({ name: 1 }, { unique: true });
crm.customers.createIndex({ tier: 1, status: 1 });
crm.customers.createIndex({ 'billingAddress.country': 1 });

const firstNames = ['Maya','Leo','Priya','Omar','Nina','Sven','Yuki','Rafael','Zoe','Asha','Klaus','Fatima','Diego','Lena','Tariq'];
const lastNames  = ['Lopez','Chen','Patel','Ivanova','Okonkwo','Becker','Sato','Ferreira','Moreau','Khan','Weiss','Silva','Kim','Rossi','Abadi'];
const titles     = ['CTO','VP Engineering','Director of Ops','Head of IT','Procurement Lead','Platform Architect','CFO','COO','CEO','CISO'];
const contacts = [];
customers.forEach((cust, i) => {
  const contactCount = 1 + (i % 3); // 1..3 contacts per customer
  for (let k = 0; k < contactCount; k++) {
    const fn = firstNames[(i * 3 + k) % firstNames.length];
    const ln = lastNames[(i * 5 + k) % lastNames.length];
    contacts.push({
      _id: ObjectId(),
      customerId: cust._id,
      firstName: fn,
      lastName: ln,
      email: `${fn}.${ln}.${i}${k}@${cust.name.toLowerCase().replace(/[^a-z]/g,'')}.test`,
      title: titles[(i + k) % titles.length],
      phone: `+1-555-${String(100 + i).padStart(3,'0')}-${String(2000 + k).padStart(4,'0')}`,
      primary: k === 0,
      optedInMarketing: (i + k) % 2 === 0,
      createdAt: daysAgo(600 - i * 10 - k)
    });
  }
});
crm.contacts.insertMany(contacts);
crm.contacts.createIndex({ customerId: 1 });
crm.contacts.createIndex({ email: 1 }, { unique: true });

const stages = ['prospect','qualified','proposal','negotiation','closed_won','closed_lost'];
const deals = customers.flatMap((cust, i) => {
  const n = 1 + (i % 3);
  return Array.from({ length: n }, (_, k) => {
    const stage = stages[(i + k) % stages.length];
    const amount = 5000 + ((i * 7 + k) % 23) * 2500;
    return {
      _id: ObjectId(),
      customerId: cust._id,
      title: `${cust.name} — ${['Platform','Addon','Renewal','Expansion','Pilot'][k % 5]} ${2024 + (k % 2)}`,
      stage,
      amount: money(amount),
      currency: 'USD',
      probability: [10, 30, 50, 70, 100, 0][stages.indexOf(stage)],
      expectedCloseAt: daysAgo(-(30 + (i + k) * 3)),
      ownerUsername: ['alice.nguyen','bob.hart','carmen.diaz','iris.kovacs','juan.reyes'][(i + k) % 5],
      source: ['inbound','outbound','referral','event'][(i + k) % 4],
      createdAt: daysAgo(180 - k * 15),
      updatedAt: daysAgo(k)
    };
  });
});
crm.deals.insertMany(deals);
crm.deals.createIndex({ customerId: 1 });
crm.deals.createIndex({ stage: 1, expectedCloseAt: 1 });
crm.deals.createIndex({ ownerUsername: 1 });

const activityTypes = ['call','email','meeting','note','task'];
const activities = deals.flatMap((deal, i) => {
  const n = 2 + (i % 4);
  return Array.from({ length: n }, (_, k) => ({
    _id: ObjectId(),
    dealId: deal._id,
    customerId: deal.customerId,
    type: activityTypes[(i + k) % activityTypes.length],
    subject: ['Intro call','Follow-up email','Technical deep-dive','Pricing discussion','Contract review','Demo session'][(i + k) % 6],
    body: `Notes from ${activityTypes[(i + k) % activityTypes.length]} #${k} about ${deal.title}.`,
    occurredAt: daysAgo(90 - (i + k)),
    durationMinutes: [null, 15, 30, 45, 60][(i + k) % 5],
    byUsername: ['alice.nguyen','bob.hart','carmen.diaz','dmitri.ivanov','iris.kovacs'][(i + k) % 5],
    attachments: (i + k) % 5 === 0 ? [{ name: 'proposal.pdf', sizeKb: 248 }] : []
  }));
});
crm.activities.insertMany(activities);
crm.activities.createIndex({ dealId: 1, occurredAt: -1 });
crm.activities.createIndex({ customerId: 1, occurredAt: -1 });

print(`[acme_crm] users=${crm.users.countDocuments()} customers=${crm.customers.countDocuments()} contacts=${crm.contacts.countDocuments()} deals=${crm.deals.countDocuments()} activities=${crm.activities.countDocuments()}`);

// ─────────────────────────────────────────────────────────────
// DB 2: acme_ops — internal operations
// ─────────────────────────────────────────────────────────────
const ops = db.getSiblingDB('acme_ops');

const departments = [
  { _id: ObjectId(), code: 'ENG', name: 'Engineering',      headcount: 84, budget: money(12500000), costCenter: 'CC-100' },
  { _id: ObjectId(), code: 'PRD', name: 'Product',          headcount: 22, budget: money(3200000),  costCenter: 'CC-110' },
  { _id: ObjectId(), code: 'SAL', name: 'Sales',            headcount: 48, budget: money(7800000),  costCenter: 'CC-200' },
  { _id: ObjectId(), code: 'MKT', name: 'Marketing',        headcount: 19, budget: money(4100000),  costCenter: 'CC-210' },
  { _id: ObjectId(), code: 'SUP', name: 'Customer Support', headcount: 31, budget: money(2600000),  costCenter: 'CC-220' },
  { _id: ObjectId(), code: 'FIN', name: 'Finance',          headcount: 11, budget: money(1800000),  costCenter: 'CC-300' },
  { _id: ObjectId(), code: 'HR',  name: 'People Ops',       headcount: 9,  budget: money(1200000),  costCenter: 'CC-310' }
];
ops.departments.insertMany(departments);
ops.departments.createIndex({ code: 1 }, { unique: true });

const empFirst = ['Anh','Blake','Chidi','Diana','Eitan','Fen','Gabe','Hana','Ida','Jae','Kwame','Lior','Mei','Noor','Owen','Petra','Quinn','Rafa','Sana','Tomas'];
const empLast  = ['Alvarez','Blum','Chowdhury','Dimitriou','Eriksen','Faulkner','Garcia','Hoffman','Ibrahim','Jansen','Kagan','Lindqvist','Matsuda','Nwosu','Oduya','Park','Quist','Rinaldi','Saito','Tchalla'];
const employees = Array.from({ length: 40 }, (_, i) => {
  const dept = departments[i % departments.length];
  const fn = empFirst[i % empFirst.length];
  const ln = empLast[(i * 3) % empLast.length];
  const salary = 65000 + ((i * 13) % 14) * 8000;
  return {
    _id: ObjectId(),
    employeeNumber: 'E' + String(1000 + i),
    firstName: fn,
    lastName: ln,
    email: `${fn}.${ln}.${i}@acme.test`.toLowerCase(),
    departmentCode: dept.code,
    title: ['Engineer','Senior Engineer','Staff Engineer','Manager','Director','Specialist','Analyst'][(i * 2) % 7],
    level: (i % 6) + 1,
    salary: money(salary),
    currency: 'USD',
    hiredAt: daysAgo(2000 - i * 40),
    terminatedAt: i % 11 === 0 ? daysAgo(i * 3) : null,
    manager: i > 0 ? 'E' + String(1000 + (i % 5)) : null,
    location: ['SFO','NYC','BER','LON','TYO','REMOTE'][i % 6],
    skills: [['java','kotlin','mongodb'],['python','sql','airflow'],['react','typescript','css'],['go','grpc','kafka'],['excel','sql','tableau']][i % 5]
  };
});
ops.employees.insertMany(employees);
ops.employees.createIndex({ employeeNumber: 1 }, { unique: true });
ops.employees.createIndex({ email: 1 }, { unique: true });
ops.employees.createIndex({ departmentCode: 1 });
ops.employees.createIndex({ manager: 1 });

const projects = [
  { code: 'APOLLO',   name: 'Data platform v2',     owner: 'ENG', status: 'active',    budget: money(950000) },
  { code: 'HERMES',   name: 'Partner API rollout',  owner: 'ENG', status: 'active',    budget: money(420000) },
  { code: 'ATLAS',    name: 'Mongo Atlas migration',owner: 'ENG', status: 'planned',   budget: money(280000) },
  { code: 'ORION',    name: 'Marketing site refresh',owner:'MKT', status: 'active',    budget: money(140000) },
  { code: 'ZEUS',     name: 'Sales enablement',     owner: 'SAL', status: 'on_hold',   budget: money(95000) },
  { code: 'HADES',    name: 'Legacy system sunset', owner: 'ENG', status: 'active',    budget: money(210000) },
  { code: 'ARTEMIS',  name: 'Customer portal',      owner: 'PRD', status: 'completed', budget: money(380000) },
  { code: 'POSEIDON', name: 'Compliance audit',     owner: 'FIN', status: 'active',    budget: money(170000) }
];
const projectDocs = projects.map((p, i) => ({
  _id: ObjectId(),
  ...p,
  startedAt: daysAgo(300 - i * 20),
  endsAt: p.status === 'completed' ? daysAgo(i * 5) : daysAgo(-(90 + i * 10)),
  members: employees.filter((_, idx) => idx % projects.length === i).slice(0, 6).map(e => e.employeeNumber),
  risks: i % 3 === 0 ? ['schedule','scope'] : ['scope']
}));
ops.projects.insertMany(projectDocs);
ops.projects.createIndex({ code: 1 }, { unique: true });
ops.projects.createIndex({ status: 1 });
ops.projects.createIndex({ owner: 1 });

const taskStatuses = ['todo','in_progress','blocked','review','done'];
const tasks = projectDocs.flatMap((proj, i) => {
  const n = 4 + (i % 4);
  return Array.from({ length: n }, (_, k) => ({
    _id: ObjectId(),
    projectCode: proj.code,
    key: `${proj.code}-${k + 1}`,
    title: [`Design ${proj.code} schema`, `Implement ${proj.code} API`, `Write ${proj.code} docs`, `Load test ${proj.code}`, `Deploy ${proj.code} to staging`, `Security review ${proj.code}`, `QA sign-off ${proj.code}`][(i + k) % 7],
    status: taskStatuses[(i + k) % taskStatuses.length],
    priority: ['low','medium','high','critical'][(i + k) % 4],
    assignee: proj.members[k % Math.max(1, proj.members.length)] || null,
    storyPoints: [1, 2, 3, 5, 8, 13][(i + k) % 6],
    labels: k % 2 === 0 ? ['backend'] : ['frontend','urgent'],
    createdAt: daysAgo(200 - (i + k) * 3),
    dueAt: daysAgo(-(7 + k * 4))
  }));
});
ops.tasks.insertMany(tasks);
ops.tasks.createIndex({ key: 1 }, { unique: true });
ops.tasks.createIndex({ projectCode: 1, status: 1 });
ops.tasks.createIndex({ assignee: 1 });

const timesheets = [];
employees.slice(0, 25).forEach((emp, i) => {
  for (let d = 0; d < 6; d++) {
    const proj = projectDocs[(i + d) % projectDocs.length];
    timesheets.push({
      _id: ObjectId(),
      employeeNumber: emp.employeeNumber,
      projectCode: proj.code,
      taskKey: `${proj.code}-${(d % 4) + 1}`,
      date: daysAgo(d * 2 + (i % 5)),
      hours: NumberDecimal(((4 + ((i + d) % 5)) + 0.25 * ((i + d) % 4)).toFixed(2)),
      billable: (i + d) % 3 !== 0,
      note: `Work on ${proj.name}`,
      approved: (i + d) % 4 !== 0
    });
  }
});
ops.timesheets.insertMany(timesheets);
ops.timesheets.createIndex({ employeeNumber: 1, date: -1 });
ops.timesheets.createIndex({ projectCode: 1, date: -1 });

print(`[acme_ops] departments=${ops.departments.countDocuments()} employees=${ops.employees.countDocuments()} projects=${ops.projects.countDocuments()} tasks=${ops.tasks.countDocuments()} timesheets=${ops.timesheets.countDocuments()}`);
print('Seed complete.');
