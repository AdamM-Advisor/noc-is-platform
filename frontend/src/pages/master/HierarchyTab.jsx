import { useState, useEffect, useCallback, useMemo } from 'react';
import { ChevronRight, ChevronDown, Plus, Edit, Ban, Search, Save, X } from 'lucide-react';
import client from '../../api/client';

const LEVELS = ['area', 'regional', 'nop', 'to'];
const LEVEL_LABELS = { area: 'Area', regional: 'Regional', nop: 'NOP', to: 'TO' };
const LEVEL_COLORS = {
  area: 'bg-blue-100 text-blue-800',
  regional: 'bg-green-100 text-green-800',
  nop: 'bg-yellow-100 text-yellow-800',
  to: 'bg-purple-100 text-purple-800',
};

function getNodeInfo(node) {
  if (node.area_id && !node.regional_id) return { level: 'area', id: node.area_id, name: node.area_name };
  if (node.regional_id) return { level: 'regional', id: node.regional_id, name: node.regional_name };
  if (node.nop_id) return { level: 'nop', id: node.nop_id, name: node.nop_name };
  if (node.to_id) return { level: 'to', id: node.to_id, name: node.to_name };
  return { level: 'area', id: '', name: '' };
}

function getAliases(node, level) {
  if (level === 'area') return { alias: node.area_alias || '' };
  const prefix = level;
  return {
    alias_site_master: node[`${prefix}_alias_site_master`] || '',
    alias_ticket: node[`${prefix}_alias_ticket`] || '',
  };
}

function matchesSearch(node, search) {
  const info = getNodeInfo(node);
  if (info.name.toLowerCase().includes(search)) return true;
  if (node.children) return node.children.some((c) => matchesSearch(c, search));
  return false;
}

function TreeNode({ node, depth, selected, onSelect, expanded, onToggle, search }) {
  const info = getNodeInfo(node);
  const hasChildren = node.children && node.children.length > 0;
  const isExpanded = expanded[`${info.level}-${info.id}`];
  const isSelected = selected && selected.level === info.level && selected.id === info.id;
  const childCount = node.children ? node.children.length : 0;

  const filteredChildren = useMemo(() => {
    if (!search || !node.children) return node.children || [];
    return node.children.filter((c) => matchesSearch(c, search));
  }, [node.children, search]);

  if (search && !matchesSearch(node, search)) return null;

  return (
    <div>
      <div
        className={`flex items-center gap-1 px-2 py-1.5 cursor-pointer rounded-md text-sm hover:bg-gray-100 transition-colors ${
          isSelected ? 'bg-blue-50 border-l-2 border-[#1B2A4A]' : ''
        }`}
        style={{ paddingLeft: `${depth * 20 + 8}px` }}
        onClick={() => onSelect({ ...node, _level: info.level, _id: info.id, _name: info.name })}
      >
        {hasChildren ? (
          <button
            className="p-0.5 hover:bg-gray-200 rounded"
            onClick={(e) => {
              e.stopPropagation();
              onToggle(`${info.level}-${info.id}`);
            }}
          >
            {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
        ) : (
          <span className="w-5" />
        )}
        <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${LEVEL_COLORS[info.level]}`}>
          {LEVEL_LABELS[info.level]}
        </span>
        <span className={`flex-1 truncate ${node.status === 'INACTIVE' ? 'text-gray-400 line-through' : 'text-gray-800'}`}>
          {info.name}
        </span>
        {hasChildren && (
          <span className="text-xs text-gray-400 bg-gray-100 px-1.5 rounded-full">{childCount}</span>
        )}
      </div>
      {isExpanded && filteredChildren.map((child) => {
        const ci = getNodeInfo(child);
        return (
          <TreeNode
            key={`${ci.level}-${ci.id}`}
            node={child}
            depth={depth + 1}
            selected={selected}
            onSelect={onSelect}
            expanded={expanded}
            onToggle={onToggle}
            search={search}
          />
        );
      })}
    </div>
  );
}

function AddModal({ open, onClose, onSave, tree }) {
  const [level, setLevel] = useState('area');
  const [formData, setFormData] = useState({});

  useEffect(() => {
    setFormData({});
  }, [level, open]);

  if (!open) return null;

  const parentLevel = level === 'regional' ? 'area' : level === 'nop' ? 'regional' : level === 'to' ? 'nop' : null;

  const getParentOptions = () => {
    if (!parentLevel || !tree) return [];
    if (parentLevel === 'area') return tree.map((a) => ({ id: a.area_id, name: a.area_name }));
    if (parentLevel === 'regional') {
      const opts = [];
      tree.forEach((a) => (a.children || []).forEach((r) => opts.push({ id: r.regional_id, name: r.regional_name })));
      return opts;
    }
    if (parentLevel === 'nop') {
      const opts = [];
      tree.forEach((a) => (a.children || []).forEach((r) => (r.children || []).forEach((n) => opts.push({ id: n.nop_id, name: n.nop_name }))));
      return opts;
    }
    return [];
  };

  const parentOptions = getParentOptions();

  const handleSubmit = () => {
    const body = { status: 'ACTIVE', ...formData };
    onSave(level, body);
  };

  const set = (k, v) => setFormData((p) => ({ ...p, [k]: v }));

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">Tambah Data Hierarki</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
        </div>
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Level</label>
            <select
              value={level}
              onChange={(e) => setLevel(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
            >
              {LEVELS.map((l) => <option key={l} value={l}>{LEVEL_LABELS[l]}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">ID</label>
            <input
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
              value={formData[`${level}_id`] || ''}
              onChange={(e) => set(`${level}_id`, e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Nama</label>
            <input
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
              value={formData[`${level}_name`] || ''}
              onChange={(e) => set(`${level}_name`, e.target.value)}
            />
          </div>
          {parentLevel && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Parent ({LEVEL_LABELS[parentLevel]})</label>
              <select
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                value={formData[`${parentLevel}_id`] || ''}
                onChange={(e) => set(`${parentLevel}_id`, e.target.value)}
              >
                <option value="">Pilih {LEVEL_LABELS[parentLevel]}</option>
                {parentOptions.map((o) => <option key={o.id} value={o.id}>{o.name}</option>)}
              </select>
            </div>
          )}
          {level === 'area' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Alias</label>
              <input
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                value={formData.area_alias || ''}
                onChange={(e) => set('area_alias', e.target.value)}
              />
            </div>
          )}
          {level !== 'area' && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Alias Site Master</label>
                <input
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                  value={formData[`${level}_alias_site_master`] || ''}
                  onChange={(e) => set(`${level}_alias_site_master`, e.target.value)}
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Alias Ticket</label>
                <input
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                  value={formData[`${level}_alias_ticket`] || ''}
                  onChange={(e) => set(`${level}_alias_ticket`, e.target.value)}
                />
              </div>
            </>
          )}
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 border border-gray-300 rounded-md hover:bg-gray-50">Batal</button>
          <button onClick={handleSubmit} className="px-4 py-2 text-sm text-white bg-[#1B2A4A] rounded-md hover:bg-[#2a3d66] flex items-center gap-1">
            <Plus size={14} /> Simpan
          </button>
        </div>
      </div>
    </div>
  );
}

function HierarchyTab() {
  const [tree, setTree] = useState([]);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState({});
  const [selected, setSelected] = useState(null);
  const [search, setSearch] = useState('');
  const [showAdd, setShowAdd] = useState(false);
  const [editMode, setEditMode] = useState(false);
  const [editData, setEditData] = useState({});
  const [saving, setSaving] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [treeRes, statsRes] = await Promise.all([
        client.get('/master/hierarchy/tree'),
        client.get('/master/hierarchy/stats'),
      ]);
      setTree(treeRes.data || []);
      setStats(statsRes.data || {});
    } catch (e) {
      /* handled by interceptor */
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const toggleExpand = (key) => {
    setExpanded((p) => ({ ...p, [key]: !p[key] }));
  };

  const handleSelect = (node) => {
    const level = node._level;
    const aliases = getAliases(node, level);
    setSelected({ ...node, level, id: node._id, name: node._name });
    setEditData(aliases);
    setEditMode(false);
  };

  const handleSaveAlias = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      const level = selected.level;
      const id = selected.id;
      const body = {};
      if (level === 'area') {
        body.area_alias = editData.alias || '';
      } else {
        body[`${level}_alias_site_master`] = editData.alias_site_master || '';
        body[`${level}_alias_ticket`] = editData.alias_ticket || '';
      }
      await client.put(`/master/${level}/${id}`, body);
      await fetchData();
    } catch (e) { /* interceptor */ }
    setSaving(false);
    setEditMode(false);
  };

  const handleDeactivate = async () => {
    if (!selected) return;
    if (!window.confirm(`Nonaktifkan ${LEVEL_LABELS[selected.level]} "${selected.name}"?`)) return;
    try {
      await client.delete(`/master/${selected.level}/${selected.id}`, { data: { cascade: true } });
      setSelected(null);
      await fetchData();
    } catch (e) { /* interceptor */ }
  };

  const handleCreate = async (level, body) => {
    try {
      await client.post(`/master/${level}`, body);
      setShowAdd(false);
      await fetchData();
    } catch (e) { /* interceptor */ }
  };

  const handleEdit = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      const level = selected.level;
      const body = { ...editData };
      await client.put(`/master/${level}/${selected.id}`, body);
      await fetchData();
      setEditMode(false);
    } catch (e) { /* interceptor */ }
    setSaving(false);
  };

  const searchLower = search.toLowerCase();

  const selectedChildCount = selected?.children ? selected.children.length : 0;

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between mb-4">
        <div className="relative flex-1 max-w-sm">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Cari hierarki..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
          />
        </div>
        <button
          onClick={() => setShowAdd(true)}
          className="ml-3 px-4 py-2 text-sm text-white bg-[#1B2A4A] rounded-md hover:bg-[#2a3d66] flex items-center gap-1"
        >
          <Plus size={14} /> Tambah
        </button>
      </div>

      <div className="flex flex-1 gap-4 min-h-0">
        <div className="w-1/2 border border-gray-200 rounded-lg overflow-auto bg-white">
          {loading ? (
            <div className="flex items-center justify-center h-40 text-gray-400 text-sm">Memuat data...</div>
          ) : tree.length === 0 ? (
            <div className="flex items-center justify-center h-40 text-gray-400 text-sm">Tidak ada data hierarki</div>
          ) : (
            <div className="p-2">
              {tree.map((node) => {
                const info = getNodeInfo(node);
                return (
                  <TreeNode
                    key={`${info.level}-${info.id}`}
                    node={node}
                    depth={0}
                    selected={selected}
                    onSelect={handleSelect}
                    expanded={expanded}
                    onToggle={toggleExpand}
                    search={searchLower}
                  />
                );
              })}
            </div>
          )}
        </div>

        <div className="w-1/2 border border-gray-200 rounded-lg bg-white p-4 overflow-auto">
          {selected ? (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <span className={`text-xs px-2 py-0.5 rounded font-medium ${LEVEL_COLORS[selected.level]}`}>
                    {LEVEL_LABELS[selected.level]}
                  </span>
                  <h3 className="text-lg font-semibold text-gray-800 mt-1">{selected.name}</h3>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setEditMode(!editMode)}
                    className="p-2 text-gray-500 hover:text-[#1B2A4A] hover:bg-gray-100 rounded-md"
                    title="Edit"
                  >
                    <Edit size={16} />
                  </button>
                  <button
                    onClick={handleDeactivate}
                    className="p-2 text-gray-500 hover:text-red-600 hover:bg-red-50 rounded-md"
                    title="Nonaktifkan"
                  >
                    <Ban size={16} />
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <span className="text-gray-500">ID</span>
                  <p className="font-medium text-gray-800">{selected.id}</p>
                </div>
                <div>
                  <span className="text-gray-500">Status</span>
                  <p className={`font-medium ${selected.status === 'ACTIVE' ? 'text-green-600' : 'text-red-500'}`}>
                    {selected.status || 'ACTIVE'}
                  </p>
                </div>
                {selected.level !== 'to' && (
                  <div>
                    <span className="text-gray-500">Jumlah Child</span>
                    <p className="font-medium text-gray-800">{selectedChildCount}</p>
                  </div>
                )}
              </div>

              <div className="border-t pt-4">
                <h4 className="text-sm font-semibold text-gray-700 mb-3">Alias</h4>
                {selected.level === 'area' ? (
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">Alias</label>
                    <input
                      className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A] disabled:bg-gray-50"
                      value={editData.alias || ''}
                      onChange={(e) => setEditData((p) => ({ ...p, alias: e.target.value }))}
                      disabled={!editMode}
                    />
                  </div>
                ) : (
                  <div className="space-y-3">
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Alias Site Master</label>
                      <input
                        className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A] disabled:bg-gray-50"
                        value={editData.alias_site_master || ''}
                        onChange={(e) => setEditData((p) => ({ ...p, alias_site_master: e.target.value }))}
                        disabled={!editMode}
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Alias Ticket</label>
                      <input
                        className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A] disabled:bg-gray-50"
                        value={editData.alias_ticket || ''}
                        onChange={(e) => setEditData((p) => ({ ...p, alias_ticket: e.target.value }))}
                        disabled={!editMode}
                      />
                    </div>
                  </div>
                )}
                {editMode && (
                  <button
                    onClick={handleSaveAlias}
                    disabled={saving}
                    className="mt-3 px-4 py-2 text-sm text-white bg-[#1B2A4A] rounded-md hover:bg-[#2a3d66] flex items-center gap-1 disabled:opacity-50"
                  >
                    <Save size={14} /> {saving ? 'Menyimpan...' : 'Simpan'}
                  </button>
                )}
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-gray-400 text-sm">
              Pilih node dari hierarki untuk melihat detail
            </div>
          )}
        </div>
      </div>

      <div className="mt-4 flex items-center gap-3 text-sm text-gray-500 bg-gray-50 rounded-md px-4 py-2">
        <span className="font-medium text-gray-700">{stats.area || 0} Area</span>
        <span>|</span>
        <span className="font-medium text-gray-700">{stats.regional || 0} Regional</span>
        <span>|</span>
        <span className="font-medium text-gray-700">{stats.nop || 0} NOP</span>
        <span>|</span>
        <span className="font-medium text-gray-700">{stats.to || 0} TO</span>
      </div>

      <AddModal open={showAdd} onClose={() => setShowAdd(false)} onSave={handleCreate} tree={tree} />
    </div>
  );
}

export default HierarchyTab;
