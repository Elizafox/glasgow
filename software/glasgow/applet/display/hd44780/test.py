from unittest.mock import AsyncMock, Mock, patch

from amaranth.hdl import Fragment
from amaranth.lib import io
from amaranth.sim import Simulator

from glasgow.gateware.ports import PortGroup
from glasgow.applet import GlasgowAppletV2TestCase, synthesis_test, async_test
from . import DisplayHD44780Applet, HD44780Component, HD44780Interface


class DisplayHD44780AppletTestCase(GlasgowAppletV2TestCase, applet=DisplayHD44780Applet):
    @synthesis_test
    def test_build(self):
        self.assertBuilds()

    def make_interface(self):
        assembly = Mock()
        assembly.sys_clk_period = 1e-6
        assembly.add_port_group.return_value = PortGroup(
            rs=io.SimulationPort("o", 1), rw=io.SimulationPort("o", 1),
            e=io.SimulationPort("o", 1), d=io.SimulationPort("io", 4))

        def add_submodule(component):
            Fragment.get(component, None)
            return component

        assembly.add_submodule.side_effect = add_submodule
        assembly.add_inout_pipe.return_value = AsyncMock()
        return HD44780Interface(Mock(), assembly, rs=None, rw=None, e=None, d=[])

    @async_test
    async def test_initialize(self):
        iface = self.make_interface()
        with patch("glasgow.applet.display.hd44780.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await iface.initialize()
        sleep.assert_awaited_once_with(0.040)
        packets = [bytes(call.args[0]) for call in iface._pipe.send.await_args_list]
        self.assertEqual(packets, [
            b"\x04\x03\x08\x08\x08", b"\x04\x03\x08\x08\x08",
            b"\x04\x03\x08\x08\x08", b"\x04\x02\x02",
            b"\x00\x20\x02", b"\x00\x08\x02",
            b"\x00\x01\x02", b"\x00\x06\x02",
        ])
        self.assertEqual(iface._pipe.flush.await_count, len(packets))

        await iface.use_two_display_lines()
        await iface.use_5x10_font()
        await iface.display_on()
        await iface.cursor_on()
        await iface.display_shift_on()
        with patch("glasgow.applet.display.hd44780.asyncio.sleep", new_callable=AsyncMock):
            await iface.initialize()
        self.assertEqual([bytes(call.args[0]) for call in iface._pipe.send.await_args_list][-8:],
                         packets)
        await iface.cursor_on()
        iface._pipe.send.assert_awaited_with(b"\x00\x0a\x02")
        await iface.display_shift_off()
        iface._pipe.send.assert_awaited_with(b"\x00\x06\x02")
        await iface.set_cursor_position(position=41, line_no=1)
        iface._pipe.send.assert_awaited_with(b"\x00\xa9\x02")

    @async_test
    async def test_cursor_position(self):
        iface = self.make_interface()
        await iface.use_two_display_lines()
        await iface.set_cursor_position(position=3, line_no=1)
        iface._pipe.send.assert_awaited_with(b"\x00\xc3\x02")
        await iface.set_cursor_position(position=43, line_no=0)
        iface._pipe.send.assert_awaited_with(b"\x00\x83\x02")

    @async_test
    async def test_write_flushes(self):
        iface = self.make_interface()
        await iface.write_text("Hi")
        self.assertEqual([bytes(call.args[0]) for call in iface._pipe.send.await_args_list],
                         [b"\x01H\x02", b"\x01i\x02"])
        iface._pipe.flush.assert_awaited_once()

    def test_bus_write(self):
        ports = PortGroup(rs=io.SimulationPort("o", 1), rw=io.SimulationPort("o", 1),
                          e=io.SimulationPort("o", 1), d=io.SimulationPort("io", 4))
        dut = HD44780Component(ports=ports, sys_clk_period=1e-6)
        writes = []

        async def monitor(ctx):
            last_e = 0
            while True:
                _, _, e, rs, rw, data = await ctx.tick().sample(
                    ports.e.o, ports.rs.o, ports.rw.o, ports.d.o)
                if last_e and not e:
                    writes.append((rs, rw, data))
                last_e = e

        async def testbench(ctx):
            # Half-byte initialization, a wait, then a full data byte.
            for byte in (0x04, 0x03, 0x08, 0x01, 0x41):
                ctx.set(dut.i_stream.payload, byte)
                ctx.set(dut.i_stream.valid, 1)
                for _ in range(2000):
                    _, _, ready = await ctx.tick().sample(dut.i_stream.ready)
                    if ready:
                        break
                else:
                    self.fail("command stream stalled")
            ctx.set(dut.i_stream.valid, 0)
            await ctx.tick().repeat(30)
            self.assertEqual(writes, [(0, 0, 3), (1, 0, 4), (1, 0, 1)])

        sim = Simulator(dut)
        sim.add_clock(1e-6)
        sim.add_testbench(monitor, background=True)
        sim.add_testbench(testbench)
        sim.run()
