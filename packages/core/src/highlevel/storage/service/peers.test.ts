import type { IPeersRepository } from '../repository/peers.js'

import { createStub, StubMemoryTelegramStorage, StubTelegramClient } from '@mtcute/test'
import Long from 'long'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

describe('PeersService', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('should keep flushing pending writes after the first flush', async () => {
    const storage = new StubMemoryTelegramStorage()
    const storeSpy = vi.spyOn(storage.peers, 'store')

    const client = new StubTelegramClient({ storage })
    await client.prepare()

    const user = createStub('user', { id: 1, accessHash: Long.fromInt(123) })

    await client.storage.peers.updatePeersFrom([user])
    expect(storeSpy).toHaveBeenCalledTimes(1)

    await client.storage.peers.updatePeersFrom([user])
    await vi.advanceTimersByTimeAsync(30_000)
    expect(storeSpy).toHaveBeenCalledTimes(2)

    await client.storage.peers.updatePeersFrom([user])
    await vi.advanceTimersByTimeAsync(30_000)
    expect(storeSpy).toHaveBeenCalledTimes(3)

    await client.destroy()
  })

  it('should batch pending writes', async () => {
    const storage = new StubMemoryTelegramStorage()
    const storeMany = vi.fn((_peers: readonly IPeersRepository.PeerInfo[]) => {})
    const deleteByPeers = vi.fn((_peerIds: readonly number[]) => {})
    Object.assign(storage.peers, { storeMany })
    Object.assign(storage.refMessages, { deleteByPeers })

    const client = new StubTelegramClient({ storage })
    await client.prepare()

    const user1 = createStub('user', { id: 1, accessHash: Long.fromInt(123) })
    const user2 = createStub('user', { id: 2, accessHash: Long.fromInt(456) })
    await client.storage.peers.updatePeersFrom([user1, user2])
    storeMany.mockClear()

    await client.storage.peers.updatePeersFrom([user1, user2])
    await vi.advanceTimersByTimeAsync(30_000)

    expect(storeMany).toHaveBeenCalledOnce()
    expect(storeMany.mock.calls[0]![0]).toHaveLength(2)
    expect(deleteByPeers).toHaveBeenCalledOnce()
    expect(deleteByPeers).toHaveBeenLastCalledWith([user1.id, user2.id])

    await client.destroy()
  })
})
