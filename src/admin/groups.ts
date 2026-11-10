import type { Admin } from "kafkajs";
import type { GroupAdmin } from "./types.js";

export function createGroupAdmin(admin: Admin): GroupAdmin {
    return {
        async listGroups() {
            const result = (await admin.listGroups()) as unknown as {
                groups: Array<{ groupId: string; protocolType: string }>;
            };
            return result.groups.map((g) => ({
                groupId: g.groupId,
                protocolType: g.protocolType ?? null,
                protocol: null,
                state: null,
                members: [],
            }));
        },
        async describeGroups(groupIds) {
            const result = (await admin.describeGroups([...groupIds])) as unknown as {
                groups: Array<{
                    groupId: string;
                    members: Array<{
                        memberId: string;
                        clientId: string;
                        host: string;
                        memberAssignment: Buffer | null;
                    }>;
                    protocol: string;
                    protocolType: string;
                    state: string;
                }>;
            };
            return result.groups.map((g) => ({
                groupId: g.groupId,
                protocolType: g.protocolType ?? null,
                protocol: g.protocol ?? null,
                state: g.state ?? null,
                members: (g.members ?? []).map((m) => ({
                    memberId: m.memberId,
                    clientId: m.clientId,
                    host: m.host,
                    memberAssignment: m.memberAssignment ?? null,
                })),
            }));
        },
        async deleteGroups(groupIds) {
            const results = await admin.deleteGroups([...groupIds]);
            return results.map((r: { groupId: string; errorCode?: number; error?: unknown }) => ({
                groupId: r.groupId,
                errorCode: r.errorCode ?? (r.error ? 1 : 0),
                errorMessage: r.error ? String(r.error) : null,
            }));
        },
    };
}

